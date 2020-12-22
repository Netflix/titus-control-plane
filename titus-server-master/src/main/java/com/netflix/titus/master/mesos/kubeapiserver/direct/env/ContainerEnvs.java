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

package com.netflix.titus.master.mesos.kubeapiserver.direct.env;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.kubernetes.client.openapi.models.V1EnvVar;

public class ContainerEnvs {

    public static List<V1EnvVar> toV1EnvVar(Map<String, String> env) {
        return env.entrySet().stream()
                .map(entry -> new V1EnvVar().name(entry.getKey()).value(entry.getValue()))
                .collect(Collectors.toList());
    }
}
