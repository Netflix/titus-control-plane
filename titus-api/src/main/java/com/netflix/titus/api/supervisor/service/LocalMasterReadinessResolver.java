/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.supervisor.service;

import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import reactor.core.publisher.Flux;

/**
 * A given master instance can join a leader election process when multiple criteria are met. For example:
 * <ul>
 * <li>bootstrap process has finished</li>
 * <li>an instance is health/li>
 * <li>an administrator configured the instance to be part of the leader election process</li>
 * <p>
 * {@link LocalMasterReadinessResolver} implementations provide different readiness checks. A given master instance
 * is ready, if all the constituents agree on it.
 */
public interface LocalMasterReadinessResolver {

    Flux<ReadinessStatus> observeLocalMasterReadinessUpdates();
}
