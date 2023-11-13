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

import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.converter.JdbcRowConverter;
import com.starrocks.connector.flink.dialect.MySqlDialect;
import com.starrocks.connector.flink.statement.FieldNamedPreparedStatement;
import com.starrocks.connector.flink.tools.EnvUtils;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkArgument;

public class StarRocksDynamicLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicLookupFunction.class);

    private final StarRocksSourceOptions sourceOptions;
    private final int maxRetryTimes;

    // cache for lookup data
    private final String[] keyNames;

    private final String query;

    private transient StarRocksJdbcConnectionProvider connectionProvider;

    private transient FieldNamedPreparedStatement statement;

    private final JdbcRowConverter lookupKeyRowConverter;

    private final JdbcRowConverter jdbcRowConverter;

    private transient Cache<RowData, List<RowData>> cache;

    private final LookupConfig lookupConfig;


    public StarRocksDynamicLookupFunction(StarRocksSourceOptions sourceOptions,
                                          String[] fieldNames,
                                          DataType[] fieldTypes,
                                          String[] keyNames,
                                          RowType rowType,
                                          LookupConfig lookupConfig
    ) {
        this.sourceOptions = sourceOptions;

        this.maxRetryTimes = sourceOptions.getLookupMaxRetries();

        this.keyNames = keyNames;
        this.lookupConfig = lookupConfig;
        MySqlDialect mySqlDialect = new MySqlDialect();
        this.query = lookupSql(sourceOptions.getTableName(), fieldNames, keyNames);
        LOG.info("lookup query sql is : {}",query);
        this.jdbcRowConverter = mySqlDialect.getRowConverter(rowType);
        List<String> nameList = Arrays.asList(fieldNames);
        DataType[] keyTypes =
                Arrays.stream(keyNames)
                        .map(
                                s -> {
                                    checkArgument(
                                            nameList.contains(s),
                                            "keyName %s can't find in fieldNames %s.",
                                            s,
                                            nameList);
                                    return fieldTypes[nameList.indexOf(s)];
                                })
                        .toArray(DataType[]::new);

        this.lookupKeyRowConverter = mySqlDialect.getRowConverter(RowType.of(
                Arrays.stream(keyTypes)
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new)));

    }

    private String lookupSql(String tableName, String[] selectFields, String[] conditionFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "SELECT "
                + selectExpressions
                + " FROM "
                + quoteIdentifier(tableName)
                + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }

    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        this.cache =
                lookupConfig.getCacheMaxSize() == -1 || lookupConfig.getCacheExpireMs() == -1
                        ? null
                        : CacheBuilder.newBuilder()
                        .expireAfterWrite(lookupConfig.getCacheExpireMs(), TimeUnit.MILLISECONDS)
                        .maximumSize(lookupConfig.getCacheMaxSize())
                        .build();

        connectionProvider = new StarRocksJdbcConnectionProvider(
            new StarRocksJdbcConnectionOptions(
                sourceOptions.getJdbcUrl(), sourceOptions.getUsername(),
                sourceOptions.getPassword(), sourceOptions.getDatabaseName()));

        establishConnectionAndStatement();
        LOG.info("Open lookup function. {}", EnvUtils.getGitInformation());
    }

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
        List<RowData> lookup = lookup(keyRow);
        if (lookup != null) {
            for (RowData rowData : lookup) {
                collect(rowData);
            }
            cache.put(keyRow, lookup);
        }
    }

    public List<RowData> lookup(RowData keyRow) {
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                statement = lookupKeyRowConverter.toExternal(keyRow, statement);
                long start = System.currentTimeMillis();
                try (ResultSet resultSet = statement.executeQuery()) {
                    ArrayList<RowData> rows = new ArrayList<>();
                    while (resultSet.next()) {
                        RowData row = jdbcRowConverter.toInternal(resultSet);
                        rows.add(row);
                    }
                    rows.trimToSize();
                    long end = System.currentTimeMillis();
                    if (end - start > 200) {
                        LOG.warn("lookup query starrocks cost : {}ms", (end - start));
                    }
                    return rows;
                }
            } catch (SQLException e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }

                try {
                    if (!connectionProvider.isConnectionValid()) {
                        statement.close();
                        connectionProvider.close();
                        establishConnectionAndStatement();
                    }
                } catch (SQLException | ClassNotFoundException exception) {
                    LOG.error(
                            "JDBC connection is not valid, and reestablish connection failed",
                            exception);
                    throw new RuntimeException("Reestablish JDBC connection failed", exception);
                }

                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
        return Collections.emptyList();
    }


    @Override
    public void close() throws Exception {
        if (connectionProvider != null) {
            connectionProvider.close();
        }
        super.close();
    }

    private void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Connection dbConn = connectionProvider.getConnection();
        statement = FieldNamedPreparedStatement.prepareStatement(dbConn, query, keyNames);
    }
}
