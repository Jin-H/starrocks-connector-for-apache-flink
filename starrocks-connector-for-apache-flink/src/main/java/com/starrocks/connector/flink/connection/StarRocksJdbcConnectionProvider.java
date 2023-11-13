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

package com.starrocks.connector.flink.connection;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Simple JDBC connection provider.
 */
public class StarRocksJdbcConnectionProvider implements StarRocksJdbcConnectionIProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksJdbcConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final StarRocksJdbcConnectionOptions jdbcOptions;

    private transient volatile Connection connection;

    public StarRocksJdbcConnectionProvider(StarRocksJdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    try {
                        Class.forName(jdbcOptions.getCjDriverName());
                    } catch (ClassNotFoundException ex) {
                        Class.forName(jdbcOptions.getDriverName());
                    }
                    if (jdbcOptions.getUsername().isPresent()) {
                        connection = DriverManager.getConnection(jdbcOptions.getDbURL(), jdbcOptions.getUsername().orElse(null), jdbcOptions.getPassword().orElse(null));
                    } else {
                        connection = DriverManager.getConnection(jdbcOptions.getDbURL());
                    }
                    if (StringUtils.isNoneBlank(jdbcOptions.getDatabase())) {
                        connection.setSchema(jdbcOptions.getDatabase());
                    }
                }
            }
        }
        return connection;
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        close();
        return getOrEstablishConnection();
    }

    public boolean isConnectionValid() throws SQLException {
        return connection != null && connection.isValid(60);
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (isConnectionValid() && !connection.isClosed() ) {
            return connection;
        }
        try {
            Class.forName(jdbcOptions.getCjDriverName());
        } catch (ClassNotFoundException ex) {
            LOG.warn("can not found class {}, try class {}", jdbcOptions.getCjDriverName(), jdbcOptions.getDriverName());
            Class.forName(jdbcOptions.getDriverName());
        }
        if (jdbcOptions.getUsername().isPresent()) {
            connection = DriverManager.getConnection(jdbcOptions.getDbURL(), jdbcOptions.getUsername().get(),
                    jdbcOptions.getPassword().orElse(null));
        } else {
            connection = DriverManager.getConnection(jdbcOptions.getDbURL());
        }
        return connection;
    }

    @Override
    public void close() {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.error("JDBC connection close failed.", e);
        } finally {
            connection = null;
        }
    }
}
