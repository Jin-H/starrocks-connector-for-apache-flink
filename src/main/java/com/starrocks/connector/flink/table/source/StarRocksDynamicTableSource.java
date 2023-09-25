/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.table.source;

import com.starrocks.connector.flink.table.source.struct.PushDownHolder;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.*;

public class StarRocksDynamicTableSource implements ScanTableSource, LookupTableSource,
        SupportsLimitPushDown, SupportsFilterPushDown, SupportsProjectionPushDown {

    private final TableSchema flinkSchema;
    private final StarRocksSourceOptions options;
    private final PushDownHolder pushDownHolder;

    private final LookupConfig lookupConfig;


    public StarRocksDynamicTableSource(StarRocksSourceOptions options,
                                       TableSchema schema,
                                       PushDownHolder pushDownHolder,
                                       LookupConfig lookupConfig
    ) {
        this.options = options;
        this.flinkSchema = schema;
        this.pushDownHolder = pushDownHolder;
        this.lookupConfig = lookupConfig;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        StarRocksDynamicSourceFunction sourceFunction = new StarRocksDynamicSourceFunction(
                options, flinkSchema,
                this.pushDownHolder.getFilter(),
                this.pushDownHolder.getLimit(),
                this.pushDownHolder.getSelectColumns(),
                this.pushDownHolder.getColumns(),
                this.pushDownHolder.getQueryType());
        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // JDBC only support non-nested look up keys
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = flinkSchema.getFieldNames()[innerKeyArr[0]];

        }

        final RowType rowType = (RowType) flinkSchema.toRowDataType().getLogicalType();

        StarRocksDynamicLookupFunction lookupFunction =
                new StarRocksDynamicLookupFunction(
                        options,
                        flinkSchema.getFieldNames(),
                        flinkSchema.getFieldDataTypes(),
                        keyNames, rowType, lookupConfig);

        return TableFunctionProvider.of(lookupFunction);

    }

    @Override
    public DynamicTableSource copy() {
        return new StarRocksDynamicTableSource(this.options, this.flinkSchema, this.pushDownHolder, this.lookupConfig);
    }

    @Override
    public String asSummaryString() {
        return "StarRocks Table Source";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        // if columns = "*", this func will not be called, so 'selectColumns' will be null
        int[] curProjectedFields = Arrays.stream(projectedFields).mapToInt(value -> value[0])
                .toArray();
        if (curProjectedFields.length == 0) {
            this.pushDownHolder.setQueryType(StarRocksSourceQueryType.QueryCount);
            return;
        }
        this.pushDownHolder.setQueryType(StarRocksSourceQueryType.QuerySomeColumns);

        ArrayList<String> columnList = new ArrayList<>();
        ArrayList<SelectColumn> selectColumns = new ArrayList<SelectColumn>();
        for (int index : curProjectedFields) {
            String columnName = flinkSchema.getFieldName(index).get();
            columnList.add(columnName);
            selectColumns.add(new SelectColumn(columnName, index));
        }
        String columns = String.join(", ", columnList);
        this.pushDownHolder.setColumns(columns);
        this.pushDownHolder.setSelectColumns(
                selectColumns.toArray(new SelectColumn[selectColumns.size()]));
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filtersExpressions) {
        List<String> filters = new ArrayList<>();
        List<ResolvedExpression> ac = new LinkedList<>();
        List<ResolvedExpression> remain = new LinkedList<>();

        StarRocksExpressionExtractor extractor = new StarRocksExpressionExtractor();
        for (ResolvedExpression expression : filtersExpressions) {
            if (expression.getOutputDataType().equals(DataTypes.BOOLEAN())
                    && expression.getChildren().size() == 0) {
                filters.add(expression.accept(extractor) + " = true");
                ac.add(expression);
                continue;
            }
            String str = expression.accept(extractor);
            if (str == null) {
                remain.add(expression);
                continue;
            }
            filters.add(str);
            ac.add(expression);
        }
        Optional<String> filter = Optional.of(String.join(" and ", filters));
        this.pushDownHolder.setFilter(filter.get());
        return Result.of(ac, remain);
    }

    @Override
    public void applyLimit(long limit) {
        this.pushDownHolder.setLimit(limit);
    }
}
