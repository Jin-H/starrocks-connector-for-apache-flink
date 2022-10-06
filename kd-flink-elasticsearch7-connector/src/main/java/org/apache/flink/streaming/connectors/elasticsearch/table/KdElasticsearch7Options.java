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
import org.apache.flink.configuration.description.Description;
import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.code;

/**
 * Options for {@link org.apache.flink.table.factories.DynamicTableSinkFactory} for Elasticsearch.
 */
public class KdElasticsearch7Options {

    public enum SinkModeType {
        MERGE,//null值不覆盖原有值
        OVERWRITE,//null值覆盖原有值
        FIELD//通过doc中的字段判断Merge或Overwrite模式

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

    // elasticsearch source config options
    public static final ConfigOption<Integer> SCROLL_MAX_SIZE_OPTION =
        ConfigOptions.key("scan.scroll.max-size")
            .intType()
            .noDefaultValue()
            .withDescription(
                "Maximum number of hits to be returned with each Elasticsearch scroll request");

    public static final ConfigOption<Duration> SCROLL_TIMEOUT_OPTION =
        ConfigOptions.key("scan.scroll.timeout")
            .durationType()
            .noDefaultValue()
            .withDescription(
                "Amount of time Elasticsearch will keep the search context alive for scroll requests");


    // look up config options
    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
        .key("lookup.cache.max-rows")
        .longType()
        .defaultValue(-1L)
        .withDescription(
            "the max number of rows of lookup cache, over this value, the oldest rows will " +
                "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is "
                +
                "specified. Cache is not enabled as default.");
    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions
        .key("lookup.cache.ttl")
        .durationType()
        .defaultValue(Duration.ofSeconds(10))
        .withDescription("the cache time to live.");
    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions
        .key("lookup.max-retries")
        .intType()
        .defaultValue(3)
        .withDescription("the max retry times if lookup database failed.");

    public static final ConfigOption<Integer> RETRY_ON_CONFLICT = ConfigOptions
        .key("retry-on-conflict")
        .intType()
        .defaultValue(0)
        .withDescription("retry on es version conflict.");

    public static final ConfigOption<Boolean> PARTIAL_CACHE_CACHE_MISSING_KEY =
        ConfigOptions.key("lookup.partial-cache.cache-missing-key")
            .booleanType()
            .defaultValue(true)
            .withDescription(
                "Whether to store an empty value into the cache if the lookup key doesn't match any rows in the table");

    public static final ConfigOption<Duration> PARTIAL_CACHE_EXPIRE_AFTER_ACCESS =
        ConfigOptions.key("lookup.partial-cache.expire-after-access")
            .durationType()
            .noDefaultValue()
            .withDescription("Duration to expire an entry in the cache after accessing");

    public static final ConfigOption<Duration> PARTIAL_CACHE_EXPIRE_AFTER_WRITE =
        ConfigOptions.key("lookup.partial-cache.expire-after-write")
            .durationType()
            .noDefaultValue()
            .withDescription("Duration to expire an entry in the cache after writing");

    public static final ConfigOption<LookupCacheType> CACHE_TYPE =
        ConfigOptions.key("lookup.cache")
            .enumType(LookupCacheType.class)
            .defaultValue(LookupCacheType.NONE)
            .withDescription(
                Description.builder()
                    .text(
                        "The caching strategy for this lookup table, including %s, %s and %s",
                        code(LookupCacheType.NONE.toString()),
                        code(LookupCacheType.PARTIAL.toString()),
                        code(LookupCacheType.FULL.toString()))
                    .build());

    public static final ConfigOption<Boolean> IGNORE_QUERY_ELASTICSEARCH =
        ConfigOptions.key("lookup.ignore-query-failure")
            .booleanType()
            .defaultValue(false)
            .withDescription(
                "Whether to exception where query es");

    public static final ConfigOption<Duration> CONNECTION_REQUEST_TIMEOUT =
        ConfigOptions.key("connection.request-timeout")
            .durationType()
            .noDefaultValue()
            .withDescription(
                "The timeout for requesting a connection from the connection manager.");

    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
        ConfigOptions.key("connection.timeout")
            .durationType()
            .noDefaultValue()
            .withDescription("The timeout for establishing a connection.");

    public static final ConfigOption<Duration> SOCKET_TIMEOUT =
        ConfigOptions.key("socket.timeout")
            .durationType()
            .noDefaultValue()
            .withDescription(
                "The socket timeout (SO_TIMEOUT) for waiting for data or, put differently,"
                    + "a maximum period inactivity between two consecutive data packets.");


    /**
     * Types of the lookup cache.
     */
    public enum LookupCacheType {
        NONE,
        PARTIAL,
        FULL
    }

    private KdElasticsearch7Options() {
    }
}
