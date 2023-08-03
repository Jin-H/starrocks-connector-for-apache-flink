package org.apache.flink.connector.clickhouse;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseInputFormat;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.connector.clickhouse.util.FilterPushDownHelper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;

/** ClickHouse table source. */
public class ClickHouseDynamicTableSource
        implements ScanTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsFilterPushDown {

    private final ClickHouseReadOptions readOptions;

    private final Properties connectionProperties;

    private ResolvedSchema resolvedSchema;

    private String filterClause;

    private long limit = -1L;

    public ClickHouseDynamicTableSource(
            ClickHouseReadOptions readOptions,
            Properties properties,
            ResolvedSchema resolvedSchema) {
        this.readOptions = readOptions;
        this.connectionProperties = properties;
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        AbstractClickHouseInputFormat.Builder builder =
                new AbstractClickHouseInputFormat.Builder()
                        .withOptions(readOptions)
                        .withConnectionProperties(connectionProperties)
                        .withFieldNames(this.resolvedSchema.getColumnNames().toArray(new String[0]))
                        .withFieldTypes(
                                this.resolvedSchema.getColumnDataTypes().toArray(new DataType[0]))
                        .withRowDataTypeInfo(
                                runtimeProviderContext.createTypeInformation(
                                        this.resolvedSchema.toPhysicalRowDataType()))
                        .withFilterClause(filterClause)
                        .withLimit(limit);
        return InputFormatProvider.of(builder.build());
    }

    @Override
    public DynamicTableSource copy() {
        ClickHouseDynamicTableSource source =
                new ClickHouseDynamicTableSource(readOptions, connectionProperties, resolvedSchema);
        source.filterClause = filterClause;
        source.limit = limit;
        return source;
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse table source";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filterClause = FilterPushDownHelper.convert(filters);
        return Result.of(new ArrayList<>(filters), new ArrayList<>(filters));
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        FieldsDataType fields =
                (FieldsDataType)
                        DataTypeUtils.projectRow(
                                resolvedSchema.toPhysicalRowDataType(), projectedFields);
        List<String> fieldNames = new ArrayList<>();
        List<DataType> fieldDataTypes = new ArrayList<>();
        RowType topFields = (RowType) fields.getLogicalType();
        for (int i = 0; i < topFields.getFieldCount(); i++) {
            fieldNames.add(topFields.getFieldNames().get(i));
            fieldDataTypes.add(fields.getChildren().get(i));
        }
        this.resolvedSchema = ResolvedSchema.physical(fieldNames, fieldDataTypes);
    }
}
