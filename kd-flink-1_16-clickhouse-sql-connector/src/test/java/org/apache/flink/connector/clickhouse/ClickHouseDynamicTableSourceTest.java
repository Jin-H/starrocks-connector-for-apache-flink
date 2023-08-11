package org.apache.flink.connector.clickhouse;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.test.util.CollectionTestEnvironment;

import org.junit.Test;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;

public class ClickHouseDynamicTableSourceTest {

    static String URL = "clickhouse://10.165.76.42:28129";
    static String USERNAME = "default";
    static String PASSWORD = "kedacom123";
    static String DATABASE = "system";
    static String TABLE_NAME = "query_log";

    @Test
    public void testSource() throws Exception {
        CollectionTestEnvironment env = new CollectionTestEnvironment();
        createDataSource(env).print();
        env.execute();
    }

    @Test
    public void testSink() throws Exception {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("type", STRING()),
                        Column.physical("event_date", DATE()),
                        Column.physical("event_time", TIMESTAMP()),
                        Column.physical("event_time_microseconds", TIMESTAMP(6)),
                        Column.physical("query_duration_ms", DataTypes.DECIMAL(8, 0)),
                        Column.physical("databases", ARRAY(STRING())),
                        Column.physical("exception_code", DataTypes.INT()),
                        Column.physical("is_initial_query", DataTypes.SMALLINT()),
                        Column.physical("client_revision", DataTypes.BIGINT()),
                        Column.physical("distributed_depth", DataTypes.DECIMAL(8, 0)));
        ClickHouseDmlOptions options =
                new ClickHouseDmlOptions.Builder()
                        .withUrl(URL)
                        .withUsername(USERNAME)
                        .withPassword(PASSWORD)
                        .withDatabaseName(DATABASE)
                        .withTableName(TABLE_NAME)
                        .build();
        ClickHouseDynamicTableSink tableSink =
                new ClickHouseDynamicTableSink(
                        options,
                        new Properties(),
                        new String[] {},
                        new String[] {},
                        schema.toPhysicalRowDataType());
        SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        CollectionTestEnvironment env = new CollectionTestEnvironment();
        OutputFormat<RowData> outputFormat = ((OutputFormatProvider) provider).createOutputFormat();
        createDataSource(env).output(outputFormat);
        env.execute();
    }

    private DataSource createDataSource(CollectionTestEnvironment env) {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("type", STRING()),
                        Column.physical("event_date", DATE()),
                        Column.physical("event_time", TIMESTAMP()),
                        Column.physical("event_time_microseconds", TIMESTAMP(6)),
                        Column.physical("query_duration_ms", DataTypes.DECIMAL(8, 0)),
                        Column.physical("databases", ARRAY(STRING())),
                        Column.physical("exception_code", DataTypes.INT()),
                        Column.physical("is_initial_query", DataTypes.SMALLINT()),
                        Column.physical("client_revision", DataTypes.BIGINT()),
                        Column.physical("distributed_depth", DataTypes.DECIMAL(8, 0)),
                        Column.physical(
                                "ProfileEvents", DataTypes.MAP(STRING(), DataTypes.DECIMAL(8, 0))),
                        Column.physical("Settings", DataTypes.MAP(STRING(), STRING())));

        ClickHouseReadOptions options =
                new ClickHouseReadOptions.Builder()
                        .withUrl(URL)
                        .withUsername(USERNAME)
                        .withPassword(PASSWORD)
                        .withDatabaseName(DATABASE)
                        .withTableName(TABLE_NAME)
                        .build();

        ClickHouseDynamicTableSource tableSource =
                new ClickHouseDynamicTableSource(
                        options,
                        10,
                        DefaultLookupCache.newBuilder()
                                .maximumSize(1000L)
                                .expireAfterWrite(Duration.ofSeconds(10))
                                .build(),
                        new Properties(),
                        schema.toPhysicalRowDataType());
        ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(new ScanRuntimeProviderContext());
        return env.createInput(((InputFormatProvider) provider).createInputFormat());
    }
}
