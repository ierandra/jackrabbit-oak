/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

public class AzureHttpRequestLoggingTestingPolicy implements HttpPipelinePolicy {

    private static final Logger log = LoggerFactory.getLogger(AzureHttpRequestLoggingTestingPolicy.class);

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        log.info("HTTP Request: {} {}", context.getHttpRequest().getHttpMethod(), context.getHttpRequest().getUrl());

        return next.process().flatMap(httpResponse -> {
            log.info("Status code is: {}", httpResponse.getStatusCode());
            log.info("Response time: {}ms", (stopwatch.elapsed(TimeUnit.NANOSECONDS)) / 1_000_000);

            return Mono.just(httpResponse);
        });
    }
}
