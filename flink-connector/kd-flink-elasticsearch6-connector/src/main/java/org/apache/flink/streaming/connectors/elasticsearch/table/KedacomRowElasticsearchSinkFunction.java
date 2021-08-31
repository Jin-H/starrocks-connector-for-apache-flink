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

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink function for converting upserts into Elasticsearch {@link ActionRequest}s.
 */
@Internal
class KedacomRowElasticsearchSinkFunction implements ElasticsearchSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger logger =
        LoggerFactory.getLogger(KedacomRowElasticsearchSinkFunction.class);

    private final IndexGenerator indexGenerator;
    private final String docType;
    private final SerializationSchema<RowData> serializationSchema;
    private final XContentType contentType;
    private final RequestFactory requestFactory;
    private final Function<RowData, String> createKey;
    private final KedacomElasticsearchOptions.SinkModeType sinkMode; //kedacom customized

    /**
     * 序列化的时候，忽略null值
     */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.setSerializationInclusion(Include.NON_NULL);

    }

    public KedacomRowElasticsearchSinkFunction(
            IndexGenerator indexGenerator,
            @Nullable String docType, // this is deprecated in es 7+
            SerializationSchema<RowData> serializationSchema,
            XContentType contentType,
            RequestFactory requestFactory,
            Function<RowData, String> createKey,
            KedacomElasticsearchOptions.SinkModeType sinkMode) {
        this.indexGenerator = Preconditions.checkNotNull(indexGenerator);
        this.docType = docType;
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
        this.contentType = Preconditions.checkNotNull(contentType);
        this.requestFactory = Preconditions.checkNotNull(requestFactory);
        this.createKey = Preconditions.checkNotNull(createKey);
        this.sinkMode = sinkMode;
    }

    @Override
    public void open() {
        indexGenerator.open();
    }

    @Override
    public void process(RowData element, RuntimeContext ctx, RequestIndexer indexer) {
        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                processUpsert(element, indexer);
                break;
            case UPDATE_BEFORE:
            case DELETE:
                processDelete(element, indexer);
                break;
            default:
                throw new TableException("Unsupported message kind: " + element.getRowKind());
        }
    }

    private byte[] removeNull(RowData row) {
        byte[] b = serializationSchema.serialize(row);
        try {
            return objectMapper.writeValueAsBytes(new String(b, StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            logger.error("jackson serialize row-data error, row-data : {}, error-msg : {}",
                row.toString(), e.getMessage());
        }
        return null;
    }

    private void processUpsert(RowData row, RequestIndexer indexer) {

        byte[] document;
        if (KedacomElasticsearchOptions.SinkModeType.MERGE.equals(sinkMode)) {
            document = removeNull(row);
        } else if (KedacomElasticsearchOptions.SinkModeType.OVERWRITE.equals(sinkMode)) {
            document = serializationSchema.serialize(row);
        } else {
            throw new TableException("Unsupported sink.mode : " + sinkMode);
        }
        final String key = createKey.apply(row);
        if (key != null) {
            final UpdateRequest updateRequest =
                requestFactory.createUpdateRequest(
                    indexGenerator.generate(row), docType, key, contentType, document);
            indexer.add(updateRequest);
        } else {
            final IndexRequest indexRequest =
                requestFactory.createIndexRequest(
                    indexGenerator.generate(row), docType, key, contentType, document);
            indexer.add(indexRequest);
        }
    }
    private void processDelete(RowData row, RequestIndexer indexer) {
        final String key = createKey.apply(row);
        final DeleteRequest deleteRequest =
                requestFactory.createDeleteRequest(indexGenerator.generate(row), docType, key);
        indexer.add(deleteRequest);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KedacomRowElasticsearchSinkFunction that = (KedacomRowElasticsearchSinkFunction) o;
        return Objects.equals(indexGenerator, that.indexGenerator)
                && Objects.equals(docType, that.docType)
                && Objects.equals(serializationSchema, that.serializationSchema)
                && contentType == that.contentType
                && Objects.equals(requestFactory, that.requestFactory)
                && Objects.equals(createKey, that.createKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                indexGenerator,
                docType,
                serializationSchema,
                contentType,
                requestFactory,
                createKey);
    }
}
