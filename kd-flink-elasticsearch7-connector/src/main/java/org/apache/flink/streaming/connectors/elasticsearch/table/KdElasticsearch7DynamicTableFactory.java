package org.apache.flink.streaming.connectors.elasticsearch.table;

import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLASH_MAX_SIZE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.CONNECTION_MAX_RETRY_TIMEOUT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.CONNECTION_PATH_PREFIX;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FAILURE_HANDLER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FORMAT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.HOSTS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.INDEX_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.PASSWORD_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.USERNAME_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.CACHE_TYPE;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.IGNORE_QUERY_ELASTICSEARCH;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.LOOKUP_CACHE_TTL;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.LOOKUP_MAX_RETRIES;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.PARTIAL_CACHE_CACHE_MISSING_KEY;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.RETRY_ON_CONFLICT;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.SCROLL_MAX_SIZE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.SCROLL_TIMEOUT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.SINK_MODE_FIELD_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.SINK_MODE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.SinkModeType;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.LookupCacheType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.StringUtils;

/**
 * A {@link DynamicTableSinkFactory} for discovering {@link Elasticsearch7DynamicSink}. Factory for
 * creating configured instances of {@link KdElasticsearch7DynamicSource} and {@link
 * Elasticsearch7DynamicSink}.
 */
@Internal
public class KdElasticsearch7DynamicTableFactory implements DynamicTableSourceFactory,
    DynamicTableSinkFactory {

    private final Log log = LogFactory.getLog(this.getClass().getSimpleName());


    private static final Set<ConfigOption<?>> requiredOptions = Stream.of(
        HOSTS_OPTION,
        INDEX_OPTION
    ).collect(Collectors.toSet());
    private static final Set<ConfigOption<?>> optionalOptions = Stream.of(
        BULK_FLASH_MAX_SIZE_OPTION,
        BULK_FLUSH_MAX_ACTIONS_OPTION,
        BULK_FLUSH_INTERVAL_OPTION,
        BULK_FLUSH_BACKOFF_TYPE_OPTION,
        BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
        PASSWORD_OPTION,
        USERNAME_OPTION,
        SCROLL_MAX_SIZE_OPTION,
        SCROLL_TIMEOUT_OPTION,
        KEY_DELIMITER_OPTION,
        FAILURE_HANDLER_OPTION,
        FLUSH_ON_CHECKPOINT_OPTION,
        BULK_FLUSH_BACKOFF_DELAY_OPTION,
        CONNECTION_MAX_RETRY_TIMEOUT_OPTION,
        CONNECTION_PATH_PREFIX,
        SINK_MODE_OPTION,
        SINK_MODE_FIELD_OPTION,
        FORMAT_OPTION,
        LOOKUP_CACHE_MAX_ROWS,
        LOOKUP_CACHE_TTL,
        LOOKUP_MAX_RETRIES,
        RETRY_ON_CONFLICT,
        PARTIAL_CACHE_CACHE_MISSING_KEY,
        CACHE_TYPE,
        IGNORE_QUERY_ELASTICSEARCH
    ).collect(Collectors.toSet());

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this,
            context);
        final DecodingFormat<DeserializationSchema<RowData>> format = helper.discoverDecodingFormat(
            DeserializationFormatFactory.class, FORMAT_OPTION);
        helper.validate();
        Configuration configuration = new Configuration();
        context.getCatalogTable()
            .getOptions()
            .forEach(configuration::setString);
        KdElasticsearchConfiguration config = new KdElasticsearchConfiguration(configuration,
            context.getClassLoader());

        validateSource(config, configuration);

        return new KdElasticsearch7DynamicSource(
            format,
            config,
            TableSchemaUtils.getPhysicalSchema(tableSchema),
            new KdElasticsearchLookupOptions.Builder()
                .setCacheExpireMs(config.getCacheExpiredMs().toMillis())
                .setCacheMaxSize(config.getCacheMaxSize())
                .setMaxRetryTimes(config.getMaxRetryTimes())
                .build(),
            cacheMissingKey(helper.getOptions()),
            helper.getOptions().get(IGNORE_QUERY_ELASTICSEARCH)
        );
    }

    private boolean cacheMissingKey(ReadableConfig tableOptions) {
        return tableOptions
            .get(CACHE_TYPE)
            .equals(LookupCacheType.PARTIAL) && tableOptions.get(PARTIAL_CACHE_CACHE_MISSING_KEY);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        ElasticsearchValidationUtils.validatePrimaryKey(tableSchema);

        final FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);

        final EncodingFormat<SerializationSchema<RowData>> format =
            helper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT_OPTION);

        helper.validate();
        Configuration configuration = new Configuration();
        context.getCatalogTable().getOptions().forEach(configuration::setString);
        Elasticsearch7Configuration config =
            new Elasticsearch7Configuration(configuration, context.getClassLoader());

        validate(config, configuration);

        return new KdElasticsearch7DynamicSink(
            format, config, TableSchemaUtils.getPhysicalSchema(tableSchema));
    }

    private void validate(Elasticsearch7Configuration config, Configuration originalConfiguration) {
        config.getFailureHandler(); // checks if we can instantiate the custom failure handler
        config.getHosts(); // validate hosts
        validate(
            config.getIndex().length() >= 1,
            () -> String.format("'%s' must not be empty", INDEX_OPTION.key()));
        int maxActions = config.getBulkFlushMaxActions();
        validate(
            maxActions == -1 || maxActions >= 1,
            () ->
                String.format(
                    "'%s' must be at least 1. Got: %s",
                    BULK_FLUSH_MAX_ACTIONS_OPTION.key(), maxActions));
        long maxSize = config.getBulkFlushMaxByteSize();
        long mb1 = 1024 * 1024;
        validate(
            maxSize == -1 || (maxSize >= mb1 && maxSize % mb1 == 0),
            () ->
                String.format(
                    "'%s' must be in MB granularity. Got: %s",
                    BULK_FLASH_MAX_SIZE_OPTION.key(),
                    originalConfiguration
                        .get(BULK_FLASH_MAX_SIZE_OPTION)
                        .toHumanReadableString()));
        validate(
            config.getBulkFlushBackoffRetries().map(retries -> retries >= 1).orElse(true),
            () ->
                String.format(
                    "'%s' must be at least 1. Got: %s",
                    BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(),
                    config.getBulkFlushBackoffRetries().get()));
        if (config.getUsername().isPresent()
            && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            validate(
                config.getPassword().isPresent()
                    && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get()),
                () ->
                    String.format(
                        "'%s' and '%s' must be set at the same time. Got: username '%s' and password '%s'",
                        USERNAME_OPTION.key(),
                        PASSWORD_OPTION.key(),
                        config.getUsername().get(),
                        config.getPassword().orElse("")));
        }
        //region kedacom custom
        //验证 sink.mode
        Optional<SinkModeType> optional = config.config.getOptional(SINK_MODE_OPTION);
        if (!optional.isPresent()) {
            log.info("set \"sink.mode\" to default, sink.mode=OVERWRITE");
        } else {
            if (SinkModeType.FIELD.equals(optional.get())) {
                Optional<String> fieldOption = config.config.getOptional(SINK_MODE_FIELD_OPTION);
                if (!fieldOption.isPresent() || StringUtils
                    .isNullOrWhitespaceOnly(fieldOption.get())) {
                    throw new ValidationException(
                        "Elasticsearch table with sink.mode.field cannot be bull or blank When sink.mode=Field.");
                }
            }
        }
        //endregion
    }

    private void validateSource(KdElasticsearchConfiguration config,
        Configuration originalConfiguration) {
        config.getHosts(); // validate hosts
        validate(
            config.getIndex().length() >= 1,
            () -> String.format("'%s' must not be empty", INDEX_OPTION.key()));
        validate(
            config.getScrollMaxSize().map(scrollMaxSize -> scrollMaxSize >= 1).orElse(true),
            () -> String.format(
                "'%s' must be at least 1. Got: %s",
                SCROLL_MAX_SIZE_OPTION.key(),
                config.getScrollMaxSize().get())
        );
        validate(config.getScrollTimeout().map(scrollTimeout -> scrollTimeout >= 1).orElse(true),
            () -> String.format(
                "'%s' must be at least 1. Got: %s",
                SCROLL_TIMEOUT_OPTION.key(),
                config.getScrollTimeout().get())
        );
        long cacheMaxSize = config.getCacheMaxSize();
        validate(
            cacheMaxSize == -1 || cacheMaxSize >= 1,
            () -> String.format(
                "'%s' must be at least 1. Got: %s",
                LOOKUP_CACHE_MAX_ROWS.key(),
                cacheMaxSize)
        );
        validate(
            config.getCacheExpiredMs().getSeconds() >= 1,
            () -> String.format(
                "'%s' must be at least 1. Got: %s",
                LOOKUP_CACHE_TTL.key(),
                config.getCacheExpiredMs().getSeconds())
        );
        validate(
            config.getMaxRetryTimes() >= 1,
            () -> String.format(
                "'%s' must be at least 1. Got: %s",
                LOOKUP_MAX_RETRIES.key(),
                config.getMaxRetryTimes())
        );
    }

    private void validateSink(Elasticsearch7Configuration config,
        Configuration originalConfiguration) {
        config.getFailureHandler(); // checks if we can instantiate the custom failure handler
        config.getHosts(); // validate hosts
        validate(
            config.getIndex().length() >= 1,
            () -> String.format("'%s' must not be empty", INDEX_OPTION.key()));
        int maxActions = config.getBulkFlushMaxActions();
        validate(
            maxActions == -1 || maxActions >= 1,
            () -> String.format(
                "'%s' must be at least 1 character. Got: %s",
                BULK_FLUSH_MAX_ACTIONS_OPTION.key(),
                maxActions)
        );
        long maxSize = config.getBulkFlushMaxByteSize();
        long mb1 = 1024 * 1024;
        validate(
            maxSize == -1 || (maxSize >= mb1 && maxSize % mb1 == 0),
            () -> String.format(
                "'%s' must be in MB granularity. Got: %s",
                BULK_FLASH_MAX_SIZE_OPTION.key(),
                originalConfiguration.get(BULK_FLASH_MAX_SIZE_OPTION).toHumanReadableString())
        );
        validate(
            config.getBulkFlushBackoffRetries().map(retries -> retries >= 1).orElse(true),
            () -> String.format(
                "'%s' must be at least 1. Got: %s",
                BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(),
                config.getBulkFlushBackoffRetries().get())
        );
    }

    private static void validate(boolean condition, Supplier<String> message) {
        if (!condition) {
            throw new ValidationException(message.get());
        }
    }

    @Override
    public String factoryIdentifier() {
        return "kd-elasticsearch7";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return optionalOptions;
    }
}