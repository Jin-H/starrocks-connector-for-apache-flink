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

package com.starrocks.connector.flink.table.data;

public class DefaultStarRocksRowData implements StarRocksRowData {
    private String uniqueKey;
    private String database;
    private String table;
    private String row;

    public DefaultStarRocksRowData() {

    }

    public DefaultStarRocksRowData(String database, String table) {
        this.database = database;
        this.table = table;
    }

    public DefaultStarRocksRowData(String uniqueKey,
                                   String database,
                                   String table,
                                   String row) {
        this.uniqueKey = uniqueKey;
        this.database = database;
        this.table = table;
        this.row = row;
    }

    public void setUniqueKey(String uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setRow(String row) {
        this.row = row;
    }

    @Override
    public String getUniqueKey() {
        return uniqueKey;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public String getRow() {
        return row;
    }
}
