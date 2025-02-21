/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
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

package com.starrocks.data.load.stream.properties;

import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class StreamLoadTableProperties implements Serializable {

    private final String uniqueKey;
    private final String database;
    private final String table;
    private final StreamLoadDataFormat dataFormat;
    private final Map<String, String> properties;

    private final boolean enableUpsertDelete;
    private final long chunkLimit;
    private final int maxBufferRows;

    private StreamLoadTableProperties(Builder builder) {
        this.database = builder.database;
        this.table = builder.table;

        this.uniqueKey = builder.uniqueKey == null
                ? StreamLoadUtils.getTableUniqueKey(database, table)
                : builder.uniqueKey;

        this.enableUpsertDelete = builder.enableUpsertDelete;
        this.dataFormat = builder.dataFormat == null
                ? StreamLoadDataFormat.JSON
                : builder.dataFormat;

        if (dataFormat instanceof StreamLoadDataFormat.JSONFormat) {
            chunkLimit = Math.min(3221225472L, builder.chunkLimit);
        } else {
            chunkLimit = Math.min(10737418240L, builder.chunkLimit);
        }
        this.maxBufferRows = builder.maxBufferRows;
        this.properties = builder.properties;
    }

    public String getUniqueKey() {
        return uniqueKey;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public boolean isEnableUpsertDelete() {
        return enableUpsertDelete;
    }

    public StreamLoadDataFormat getDataFormat() {
        return dataFormat;
    }

    public Long getChunkLimit() {
        return chunkLimit;
    }

    public int getMaxBufferRows() {
        return maxBufferRows;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String uniqueKey;
        private String database;
        private String table;
        private String columns;

        private boolean enableUpsertDelete;
        private StreamLoadDataFormat dataFormat;
        private long chunkLimit;
        private int maxBufferRows = Integer.MAX_VALUE;

        private final Map<String, String> properties = new HashMap<>();

        private Builder() {

        }

        public Builder uniqueKey(String uniqueKey) {
            this.uniqueKey = uniqueKey;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder columns(String columns) {
            this.columns = columns;
            return this;
        }

        public Builder enableUpsertDelete(boolean enableUpsertDelete) {
            this.enableUpsertDelete = enableUpsertDelete;
            return this;
        }

        public Builder streamLoadDataFormat(StreamLoadDataFormat dataFormat) {
            this.dataFormat = dataFormat;
            return this;
        }

        public Builder chunkLimit(long chunkLimit) {
            this.chunkLimit = chunkLimit;
            return this;
        }

        public Builder maxBufferRows(int maxBufferRows) {
            this.maxBufferRows = maxBufferRows;
            return this;
        }

        public Builder addProperties(Map<String, String> properties) {
            this.properties.putAll(properties);
            return this;
        }

        public Builder addProperty(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        public StreamLoadTableProperties build() {
            if (database == null || table == null) {
                throw new IllegalArgumentException(String.format("database `%s` or table `%s` can't be null", database, table));
            }

            addProperty("db", database);
            addProperty("table", table);
            if (columns != null) {
                addProperty("columns", columns);
            }
            return new StreamLoadTableProperties(this);
        }

    }
}
