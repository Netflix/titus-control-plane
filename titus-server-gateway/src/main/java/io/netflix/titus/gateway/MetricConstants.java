/*
 * Copyright 2017 Netflix, Inc.
 *
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

package io.netflix.titus.gateway;

/**
 * Set of metric related constants that establish consistent naming convention.
 */
public class MetricConstants {

    public static final String METRIC_ROOT = "titusGateway.";
    public static final String METRIC_ENDPOINT = METRIC_ROOT + "endpoint.";
    public static final String METRIC_REWRITE = METRIC_ENDPOINT + "rewrite.";
    public static final String METRIC_PROXY = METRIC_ENDPOINT + "proxy.";
    public static final String METRIC_CLIENT_REQUEST = METRIC_ROOT + "clientRequest.";
}
