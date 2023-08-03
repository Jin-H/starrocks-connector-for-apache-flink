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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Options for {@link org.apache.flink.table.factories.DynamicTableSinkFactory} for Elasticsearch.
 */
public class KdElasticsearch6Options {

    public enum SinkModeType {
        MERGE, // null值不覆盖原有值
        OVERWRITE, // null值覆盖原有值
        FIELD // 通过doc中的字段判断Merge或Overwrite模式
    }

    public static final ConfigOption<SinkModeType> SINK_MODE_OPTION =
            ConfigOptions.key("sink.mode")
                    .enumType(SinkModeType.class)
                    .defaultValue(SinkModeType.OVERWRITE)
                    .withDescription("Elasticsearch Sink Mode , customized by Kedacom.");

    public static final ConfigOption<String> SINK_MODE_FIELD_OPTION =
            ConfigOptions.key("sink.mode.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Elasticsearch Sink Mode Field , Shift Merge&Overwrite By doc's Field , customized by Kedacom.");

    public static final ConfigOption<Integer> RETRY_ON_CONFLICT =
            ConfigOptions.key("retry-on-conflict")
                    .intType()
                    .defaultValue(0)
                    .withDescription("retry on es version conflict.");

    private KdElasticsearch6Options() {}
}
