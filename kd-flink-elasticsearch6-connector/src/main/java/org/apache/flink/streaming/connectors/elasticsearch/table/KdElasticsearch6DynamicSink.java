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

import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch6Options.SINK_MODE_FIELD_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch6Options.SINK_MODE_OPTION;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link ElasticsearchSink} from a
 * logical description.
 */
@PublicEvolving
final class KdElasticsearch6DynamicSink implements DynamicTableSink {

    @VisibleForTesting
    static final KdElasticsearch6RequestFactory REQUEST_FACTORY = new KdElasticsearch6RequestFactory();

    private final EncodingFormat<SerializationSchema<RowData>> format;
    private final TableSchema schema;
    private final Elasticsearch6Configuration config;

    public KdElasticsearch6DynamicSink(
        EncodingFormat<SerializationSchema<RowData>> format,
        Elasticsearch6Configuration config,
        TableSchema schema) {
        this(format, config, schema, (ElasticsearchSink.Builder::new));
    }

    // --------------------------------------------------------------
    // Hack to make configuration testing possible.
    //
    // The code in this block should never be used outside of tests.
    // Having a way to inject a builder we can assert the builder in
    // the test. We can not assert everything though, e.g. it is not
    // possible to assert flushing on checkpoint, as it is configured
    // on the sink itself.
    // --------------------------------------------------------------

    private final KdElasticSearchBuilderProvider builderProvider;

    @FunctionalInterface
    interface KdElasticSearchBuilderProvider {

        ElasticsearchSink.Builder<RowData> createBuilder(
            List<HttpHost> httpHosts, KdRowElasticsearch6SinkFunction upsertSinkFunction);
    }

    KdElasticsearch6DynamicSink(
        EncodingFormat<SerializationSchema<RowData>> format,
        Elasticsearch6Configuration config,
        TableSchema schema,
        KdElasticSearchBuilderProvider builderProvider) {
        this.format = format;
        this.schema = schema;
        this.config = config;
        this.builderProvider = builderProvider;
    }

    // --------------------------------------------------------------
    // End of hack to make configuration testing possible
    // --------------------------------------------------------------

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        return () -> {
            SerializationSchema<RowData> format =
                this.format.createRuntimeEncoder(context, schema.toRowDataType());
            final KdRowElasticsearch6SinkFunction upsertFunction =
                new KdRowElasticsearch6SinkFunction(
                    IndexGeneratorFactory.createIndexGenerator(config.getIndex(), schema),
                    config.getDocumentType(),
                    format,
                    XContentType.JSON,
                    REQUEST_FACTORY,
                    KeyExtractor.createKeyExtractor(schema, config.getKeyDelimiter()),
                    config.config.getOptional(SINK_MODE_OPTION)
                        .orElse(KdElasticsearch6Options.SinkModeType.OVERWRITE),
                    config.config.getOptional(SINK_MODE_FIELD_OPTION).orElse(null)
                );

            final ElasticsearchSink.Builder<RowData> builder =
                builderProvider.createBuilder(config.getHosts(), upsertFunction);

            builder.setFailureHandler(config.getFailureHandler());
            builder.setBulkFlushMaxActions(config.getBulkFlushMaxActions());
            builder.setBulkFlushMaxSizeMb((int) (config.getBulkFlushMaxByteSize() >> 20));
            builder.setBulkFlushInterval(config.getBulkFlushInterval());
            builder.setBulkFlushBackoff(config.isBulkFlushBackoffEnabled());
            config.getBulkFlushBackoffType().ifPresent(builder::setBulkFlushBackoffType);
            config.getBulkFlushBackoffRetries().ifPresent(builder::setBulkFlushBackoffRetries);
            config.getBulkFlushBackoffDelay().ifPresent(builder::setBulkFlushBackoffDelay);

            // we must overwrite the default factory which is defined with a lambda because of a bug
            // in shading lambda serialization shading see FLINK-18006
            if (config.getUsername().isPresent()
                && config.getPassword().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())
                && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get())) {
                builder.setRestClientFactory(
                    new KdAuthRestClientFactory(
                        config.getPathPrefix().orElse(null),
                        config.getUsername().get(),
                        config.getPassword().get()));
            } else {
                builder.setRestClientFactory(
                    new KdDefaultRestClientFactory(config.getPathPrefix().orElse(null)));
            }

            final ElasticsearchSink<RowData> sink = builder.build();

            if (config.isDisableFlushOnCheckpoint()) {
                sink.disableFlushOnCheckpoint();
            }

            return sink;
        };
    }

    @Override
    public DynamicTableSink copy() {
        return this;
    }

    @Override
    public String asSummaryString() {
        return "kd-elasticsearch6";
    }

    /**
     * Serializable {@link RestClientFactory} used by the sink.
     */
    @VisibleForTesting
    static class KdDefaultRestClientFactory implements RestClientFactory {

        private final String pathPrefix;

        public KdDefaultRestClientFactory(@Nullable String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            if (pathPrefix != null) {
                restClientBuilder.setPathPrefix(pathPrefix);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KdDefaultRestClientFactory that = (KdDefaultRestClientFactory) o;
            return Objects.equals(pathPrefix, that.pathPrefix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pathPrefix);
        }
    }

    /**
     * Serializable {@link RestClientFactory} used by the sink which enable authentication.
     */
    @VisibleForTesting
    static class KdAuthRestClientFactory implements RestClientFactory {

        private final String pathPrefix;
        private final String username;
        private final String password;
        private transient CredentialsProvider credentialsProvider;

        public KdAuthRestClientFactory(
            @Nullable String pathPrefix, String username, String password) {
            this.pathPrefix = pathPrefix;
            this.password = password;
            this.username = username;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            if (pathPrefix != null) {
                restClientBuilder.setPathPrefix(pathPrefix);
            }
            if (credentialsProvider == null) {
                credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            }
            restClientBuilder.setHttpClientConfigCallback(
                httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(
                        credentialsProvider));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KdAuthRestClientFactory that = (KdAuthRestClientFactory) o;
            return Objects.equals(pathPrefix, that.pathPrefix)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pathPrefix, username, password);
        }
    }

    /**
     * Version-specific creation of {@link ActionRequest}s used by the sink.
     */
    private static class KdElasticsearch6RequestFactory implements RequestFactory {

        @Override
        public UpdateRequest createUpdateRequest(
            String index,
            String docType,
            String key,
            XContentType contentType,
            byte[] document) {
            return new UpdateRequest(index, docType, key)
                .doc(document, contentType)
                .upsert(document, contentType);
        }

        @Override
        public IndexRequest createIndexRequest(
            String index,
            String docType,
            String key,
            XContentType contentType,
            byte[] document) {
            return new IndexRequest(index, docType, key).source(document, contentType);
        }

        @Override
        public DeleteRequest createDeleteRequest(String index, String docType, String key) {
            return new DeleteRequest(index, docType, key);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KdElasticsearch6DynamicSink that = (KdElasticsearch6DynamicSink) o;
        return Objects.equals(format, that.format)
            && Objects.equals(schema, that.schema)
            && Objects.equals(config, that.config)
            && Objects.equals(builderProvider, that.builderProvider);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, schema, config, builderProvider);
    }
}
