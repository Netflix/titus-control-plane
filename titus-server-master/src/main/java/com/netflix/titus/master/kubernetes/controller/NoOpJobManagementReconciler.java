/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.controller;

import javax.inject.Singleton;

import com.netflix.titus.master.kubernetes.client.model.PodEvent;
import reactor.core.publisher.Flux;

@Singleton
public class NoOpJobManagementReconciler implements KubeJobManagementReconciler {

    @Override
    public Flux<PodEvent> getPodEventSource() {
        return Flux.empty();
    }
}
