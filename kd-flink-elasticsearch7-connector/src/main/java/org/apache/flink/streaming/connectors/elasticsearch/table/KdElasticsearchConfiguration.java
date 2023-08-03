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

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.http.HttpHost;

import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.HOSTS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.LOOKUP_CACHE_TTL;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.LOOKUP_MAX_RETRIES;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.SCROLL_MAX_SIZE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.KdElasticsearch7Options.SCROLL_TIMEOUT_OPTION;

/** Accessor methods to elasticsearch options. */
@Internal
public class KdElasticsearchConfiguration extends ElasticsearchConfiguration {

    public List<HttpHost> getHosts() {
        return config.get(HOSTS_OPTION).stream()
                .map(KdElasticsearchConfiguration::validateAndParseHostsString)
                .collect(Collectors.toList());
    }

    private static HttpHost validateAndParseHostsString(String host) {
        try {
            HttpHost httpHost = HttpHost.create(host);
            if (httpHost.getPort() < 0) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing port.",
                                host, ElasticsearchOptions.HOSTS_OPTION.key()));
            } else if (httpHost.getSchemeName() == null) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing scheme.",
                                host, ElasticsearchOptions.HOSTS_OPTION.key()));
            } else {
                return httpHost;
            }
        } catch (Exception var2) {
            throw new ValidationException(
                    String.format(
                            "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'.",
                            host, ElasticsearchOptions.HOSTS_OPTION.key()),
                    var2);
        }
    }

    KdElasticsearchConfiguration(ReadableConfig config, ClassLoader classLoader) {
        super(config, classLoader);
    }

    public Optional<Integer> getScrollMaxSize() {
        return config.getOptional(SCROLL_MAX_SIZE_OPTION);
    }

    public Optional<Long> getScrollTimeout() {
        return config.getOptional(SCROLL_TIMEOUT_OPTION).map(Duration::toMillis);
    }

    public long getCacheMaxSize() {
        return config.get(LOOKUP_CACHE_MAX_ROWS);
    }

    public Duration getCacheExpiredMs() {
        return config.get(LOOKUP_CACHE_TTL);
    }

    public int getMaxRetryTimes() {
        return config.get(LOOKUP_MAX_RETRIES);
    }
}
