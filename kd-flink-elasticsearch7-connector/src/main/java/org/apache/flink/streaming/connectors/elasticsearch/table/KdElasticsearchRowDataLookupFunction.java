/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch.table;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.connectors.kd.ElasticsearchApiCallBridge;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lookup function for ElasticsearchSource.
 */
@Internal
public class KdElasticsearchRowDataLookupFunction<C extends AutoCloseable> extends
    TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(
        KdElasticsearchRowDataLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<RowData> deserializationSchema;

    private final String index;
    private final String type;

    private final String[] producedNames;
    private final String[] lookupKeys;
    // converters to convert data from internal to external in order to generate keys for the cache.
    private final DataFormatConverter[] converters;
    private SearchRequest searchRequest;
    private SearchSourceBuilder searchSourceBuilder;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final ElasticsearchApiCallBridge<C> callBridge;

    private transient C client;
    private transient Cache<RowData, List<RowData>> cache;

    private final boolean cacheMissingKey;

    private final boolean ignoreQueryEs;

    public KdElasticsearchRowDataLookupFunction(
        DeserializationSchema<RowData> deserializationSchema,
        KdElasticsearchLookupOptions lookupOptions,
        String index,
        String type,
        String[] producedNames,
        DataType[] producedTypes,
        String[] lookupKeys,
        ElasticsearchApiCallBridge<C> callBridge,
        boolean cacheMissingKey, boolean ignoreQueryEs) {

        checkNotNull(deserializationSchema, "No DeserializationSchema supplied.");
        checkNotNull(lookupOptions, "No ElasticsearchLookupOptions supplied.");
        checkNotNull(producedNames, "No fieldNames supplied.");
        checkNotNull(producedTypes, "No fieldTypes supplied.");
        checkNotNull(lookupKeys, "No keyNames supplied.");
        checkNotNull(callBridge, "No ElasticsearchApiCallBridge supplied.");

        this.deserializationSchema = deserializationSchema;
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();

        this.index = index;
        this.type = type;
        this.producedNames = producedNames;
        this.lookupKeys = lookupKeys;
        this.converters = new DataFormatConverter[lookupKeys.length];
        this.cacheMissingKey = cacheMissingKey;
        this.ignoreQueryEs = ignoreQueryEs;
        Map<String, Integer> nameToIndex = IntStream.range(0, producedNames.length).boxed().collect(
            Collectors.toMap(i -> producedNames[i], i -> i));
        for (int i = 0; i < lookupKeys.length; i++) {
            Integer position = nameToIndex.get(lookupKeys[i]);
            Preconditions.checkArgument(position != null, "Lookup keys %s not selected",
                Arrays.toString(lookupKeys));
            converters[i] = DataFormatConverters.getConverterForDataType(producedTypes[position]);
        }

        this.callBridge = callBridge;
    }

    @Override
    public void open(FunctionContext context) {
        this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
            .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
            .maximumSize(cacheMaxSize)
            .build();
        this.client = callBridge.createClient(null);

        //Set searchRequest in open method in case of amount of calling in eval method when every record comes.
        this.searchRequest = new SearchRequest(index);
        if (type == null) {
            searchRequest.types(Strings.EMPTY_ARRAY);
        } else {
            searchRequest.types(type);
        }
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(producedNames, null);
    }


    /**
     * This is a lookup method which is called by Flink framework in runtime.
     *
     * @param keys lookup keys
     */
    public void eval(Object... keys) {
        RowData keyRow = GenericRowData.of(keys);
        if (cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                for (RowData cachedRow : cachedRows) {
                    collect(cachedRow);
                }
                return;
            }
        }

        boolean queryWithId =
            lookupKeys != null && lookupKeys.length == 1 && "_id".equals(lookupKeys[0]);

        if (!queryWithId) {
            BoolQueryBuilder lookupCondition = new BoolQueryBuilder();
            for (int i = 0; i < lookupKeys.length; i++) {
                Object value = converters[i].toExternal(keys[i]);
                if (Objects.isNull(value) || StringUtils.isNullOrWhitespaceOnly(value.toString())) {
                    LOG.info("字段【{}】的值为空", lookupKeys[i]);
                } else {
                    lookupCondition.must(new TermQueryBuilder(lookupKeys[i], value));
                }
            }
            searchSourceBuilder.query(lookupCondition);
            searchRequest.source(searchSourceBuilder);
        }

        Tuple2<String, String[]> searchResponse;

        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            try {
                if (queryWithId) {
                    if (StringUtils.isNullOrWhitespaceOnly(String.valueOf(keys[0]))) {
                        LOG.warn("_id的值为空");
                        return;
                    }
                    long l = System.currentTimeMillis();
                    searchResponse = callBridge.get(client,
                        new GetRequest(index, String.valueOf(keys[0])));
                    long cost = System.currentTimeMillis() - l;
                    if (cost > 100) {
                        LOG.info("查询es耗时大于100ms，查询【{}】耗时：{}ms", keys[0], cost);
                    }
                } else {
                    searchResponse = callBridge.search(client, searchRequest);
                    LOG.info("请求信息：{}, 结果：{}", searchRequest.toString(),
                        JSON.toJSONString(searchResponse.f1));
                }
                if (searchResponse.f1.length > 0) {
                    String[] result = searchResponse.f1;
                    // if cache disabled
                    if (cache == null) {
                        for (String s : result) {
                            if (StringUtils.isNullOrWhitespaceOnly(s)) {
                                continue;
                            }
                            RowData row = parseSearchHit(s);
                            if (Objects.isNull(row)) {
                                continue;
                            }
                            collect(row);
                        }
                    } else { // if cache enabled
                        ArrayList<RowData> rows = new ArrayList<>();
                        for (String s : result) {
                            if (StringUtils.isNullOrWhitespaceOnly(s)) {
                                continue;
                            }
                            RowData row = parseSearchHit(s);
                            if (Objects.isNull(row)) {
                                continue;
                            }
                            collect(row);
                            rows.add(row);
                        }
                        cache.put(keyRow, rows);
                    }
                } else {
                    if (cacheMissingKey) {
                        cache.put(keyRow, Lists.newLinkedList());
                    }
                }
                break;
            } catch (Exception e) {
                LOG.error(String.format(
                    "Elasticsearch search error, retry times = %d, ignore-query-failure : %s",
                    retry, ignoreQueryEs), e);
                if (ignoreQueryEs) {
                    continue;
                }
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of Elasticsearch search failed.", e);
                }
                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    LOG.warn(
                        "Interrupted while waiting to retry failed elasticsearch search, aborting");
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    private RowData parseSearchHit(String hit) {
        if (StringUtils.isNullOrWhitespaceOnly(hit)) {
            return null;
        }
        RowData row = null;
        try {
            row = deserializationSchema.deserialize(hit.getBytes());
        } catch (IOException e) {
            LOG.error("deserialization : {}", deserializationSchema.getClass().getName());
            LOG.error("Deserialize search hit failed: {}", e.getMessage(), e);
        }

        return row;
    }
}