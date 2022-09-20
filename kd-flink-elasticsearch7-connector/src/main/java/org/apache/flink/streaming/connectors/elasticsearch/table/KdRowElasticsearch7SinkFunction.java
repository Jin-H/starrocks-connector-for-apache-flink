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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
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

/**
 * Sink function for converting upserts into Elasticsearch {@link ActionRequest}s.
 */
@Internal
class KdRowElasticsearch7SinkFunction implements ElasticsearchSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private final IndexGenerator indexGenerator;
    private final String docType;
    private final SerializationSchema<RowData> serializationSchema;
    private final XContentType contentType;
    private final RequestFactory requestFactory;
    private final Function<RowData, String> createKey;
    private final KdElasticsearch7Options.SinkModeType sinkMode; //kedacom customized
    private final String sinkModeField; //kedacom customized
    private final String mergeFlag = "1";

    private final Integer retryOnConflict;

    public KdRowElasticsearch7SinkFunction(
        IndexGenerator indexGenerator,
        @Nullable String docType, // this is deprecated in es 7+
        SerializationSchema<RowData> serializationSchema,
        XContentType contentType,
        RequestFactory requestFactory,
        Function<RowData, String> createKey,
        KdElasticsearch7Options.SinkModeType sinkMode,
        String sinkModeField,
        Integer retryOnConflict) {
        this.indexGenerator = Preconditions.checkNotNull(indexGenerator);
        this.docType = docType;
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
        this.contentType = Preconditions.checkNotNull(contentType);
        this.requestFactory = Preconditions.checkNotNull(requestFactory);
        this.createKey = Preconditions.checkNotNull(createKey);
        this.sinkMode = sinkMode;
        this.sinkModeField = sinkModeField;
        this.retryOnConflict = retryOnConflict;
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
                if (KdElasticsearch7Options.SinkModeType.MERGE.equals(sinkMode)) {
                    processMergeUpsert(element, indexer);
                } else if (KdElasticsearch7Options.SinkModeType.OVERWRITE.equals(sinkMode)) {
                    processOverwriteUpsert(element, indexer);
                } else if (KdElasticsearch7Options.SinkModeType.FIELD.equals(sinkMode)) {
                    processFieldUpsert(element, indexer, sinkModeField);
                } else {
                    throw new TableException("Unsupported sink.mode : " + sinkMode);
                }
                break;
            case UPDATE_BEFORE:
            case DELETE:
                processDelete(element, indexer);
                break;
            default:
                throw new TableException("Unsupported message kind: " + element.getRowKind());
        }
    }

    //region kedacom customized
    private byte[] handleNullByField(RowData row, String field) {
        byte[] b = serializationSchema.serialize(row);
        JSONObject jsonObject = JSON.parseObject(new String(b, StandardCharsets.UTF_8));
        //当未启用Field模式 或 数据中的flag为true时，
        if (field == null || mergeFlag.equals(jsonObject.get(field))) {
            jsonObject.remove(field);//移除flag
            return JSON.parseObject(new String(b, StandardCharsets.UTF_8)).toJSONString()
                .getBytes();
        }
        return b;
    }

    private void processFieldUpsert(RowData row, RequestIndexer indexer, String Field) {
        final byte[] document = handleNullByField(row, Field);
        final String key = createKey.apply(row);
        if (key != null) {
            final UpdateRequest updateRequest =
                requestFactory.createUpdateRequest(
                    indexGenerator.generate(row), docType, key, contentType, document);
            retryOnConflict(updateRequest);
            indexer.add(updateRequest);
        } else {
            final IndexRequest indexRequest =
                requestFactory.createIndexRequest(
                    indexGenerator.generate(row), docType, key, contentType, document);
            indexer.add(indexRequest);
        }
    }

    private byte[] removeNull(RowData row) {
        byte[] b = serializationSchema.serialize(row);
        return JSON.toJSONString(JSON.parse(new String(b, StandardCharsets.UTF_8)))
            .getBytes(StandardCharsets.UTF_8);
    }

    private void processMergeUpsert(RowData row, RequestIndexer indexer) {
        final byte[] document = removeNull(row);
        final String key = createKey.apply(row);
        if (key != null) {
            final UpdateRequest updateRequest =
                requestFactory.createUpdateRequest(
                    indexGenerator.generate(row), docType, key, contentType, document);
            retryOnConflict(updateRequest);
            indexer.add(updateRequest);
        } else {
            final IndexRequest indexRequest =
                requestFactory.createIndexRequest(
                    indexGenerator.generate(row), docType, key, contentType, document);
            indexer.add(indexRequest);
        }
    }

    private void retryOnConflict(UpdateRequest updateRequest) {
        if (retryOnConflict != null && retryOnConflict > 0) {
            updateRequest.retryOnConflict(retryOnConflict);
        }
    }

    private void processOverwriteUpsert(RowData row, RequestIndexer indexer) {
        final byte[] document = serializationSchema.serialize(row);
        final String key = createKey.apply(row);
        if (key != null) {
            final UpdateRequest updateRequest =
                requestFactory.createUpdateRequest(
                    indexGenerator.generate(row), docType, key, contentType, document);
            retryOnConflict(updateRequest);
            indexer.add(updateRequest);
        } else {
            final IndexRequest indexRequest =
                requestFactory.createIndexRequest(
                    indexGenerator.generate(row), docType, key, contentType, document);
            indexer.add(indexRequest);
        }
    }
    //endregion

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
        KdRowElasticsearch7SinkFunction that = (KdRowElasticsearch7SinkFunction) o;
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
