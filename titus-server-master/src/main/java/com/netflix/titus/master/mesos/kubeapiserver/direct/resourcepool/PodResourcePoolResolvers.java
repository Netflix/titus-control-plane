/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool;

public final class PodResourcePoolResolvers {

    public static final String RESOURCE_POOL_ELASTIC = "elastic";

    public static final String RESOURCE_POOL_ELASTIC_FARZONE_PREFIX = "elastic_farzone_";

    public static final String RESOURCE_POOL_RESERVED = "reserved";

    public static final String RESOURCE_POOL_GPU_PREFIX = "elasticGpu";
}
