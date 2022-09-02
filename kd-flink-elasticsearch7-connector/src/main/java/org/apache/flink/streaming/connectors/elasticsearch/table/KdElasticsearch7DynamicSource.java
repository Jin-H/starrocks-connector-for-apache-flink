/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticSearch7InputFormat;
import org.apache.flink.streaming.connectors.elasticsearch7.Elasticsearch7ApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

/**
 * A {@link DynamicTableSource} that describes how to create a {@link KdElasticsearch7DynamicSource}
 * from a logical description.
 */
@Internal
public class KdElasticsearch7DynamicSource implements ScanTableSource, LookupTableSource,
    SupportsProjectionPushDown {

    private final DecodingFormat<DeserializationSchema<RowData>> format;
    private final KdElasticsearchConfiguration config;
    private final KdElasticsearchLookupOptions lookupOptions;
    private TableSchema physicalSchema;

    public KdElasticsearch7DynamicSource(
        DecodingFormat<DeserializationSchema<RowData>> format,
        KdElasticsearchConfiguration config,
        TableSchema physicalSchema,
        KdElasticsearchLookupOptions lookupOptions) {
        this.format = format;
        this.config = config;
        this.physicalSchema = physicalSchema;
        this.lookupOptions = lookupOptions;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        ElasticSearch7InputFormat.Builder esInputFormatBuilder = new ElasticSearch7InputFormat.Builder();
        esInputFormatBuilder.setHttpHosts(config.getHosts());

        RestClientFactory restClientFactory = null;
        if (config.getPathPrefix().isPresent()) {
            restClientFactory = new KdElasticsearch7DynamicSink.DefaultRestClientFactory(
                config.getPathPrefix().get());
        } else {
            restClientFactory = new KdElasticsearch7DynamicSink.DefaultRestClientFactory(null);
        }

        esInputFormatBuilder.setRestClientFactory(restClientFactory);
        esInputFormatBuilder.setDeserializationSchema(
            this.format.createRuntimeDecoder(runtimeProviderContext,
                physicalSchema.toRowDataType()));
        esInputFormatBuilder.setFieldNames(physicalSchema.getFieldNames());
        esInputFormatBuilder.setIndex(config.getIndex());
        if (config.getScrollMaxSize().isPresent()) {
            esInputFormatBuilder.setScrollMaxSize(config.getScrollMaxSize().get());
        }
        if (config.getScrollTimeout().isPresent()) {
            esInputFormatBuilder.setScrollTimeout(config.getScrollTimeout().get());
        }

        /**
         * TODO: for SupportsFilterPushDown/ SupportsLimitPushDown
         * 	builder.setPredicate();
         * 	builder.setLimit();
         */

        return InputFormatProvider.of(
            esInputFormatBuilder.build()
        );
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {

        RestClientFactory restClientFactory = null;
        if (config.getPathPrefix().isPresent()) {
            restClientFactory = new KdElasticsearch7DynamicSink.DefaultRestClientFactory(
                config.getPathPrefix().get());
        } else {
            restClientFactory = new KdElasticsearch7DynamicSink.DefaultRestClientFactory(null);
        }

        Elasticsearch7ApiCallBridge elasticsearch7ApiCallBridge = new Elasticsearch7ApiCallBridge(
            config.getHosts(), restClientFactory);

        // Elasticsearch only support non-nested look up keys
        String[] lookupKeys = new String[context.getKeys().length];
        for (int i = 0; i < lookupKeys.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(innerKeyArr.length == 1,
                "ElasticSearch only support non-nested look up keys");
            lookupKeys[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }

        return TableFunctionProvider.of(new KdElasticsearchRowDataLookupFunction(
            this.format.createRuntimeDecoder(context, physicalSchema.toRowDataType()),
            lookupOptions,
            config.getIndex(),
            config.getDocumentType(),
            physicalSchema.getFieldNames(),
            physicalSchema.getFieldDataTypes(),
            lookupKeys,
            elasticsearch7ApiCallBridge)
        );
    }

    @Override
    public DynamicTableSource copy() {
        return new KdElasticsearch7DynamicSource(format, config, physicalSchema, lookupOptions);
    }

    @Override
    public String asSummaryString() {
        return "kd-elasticsearch7";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
    }
}