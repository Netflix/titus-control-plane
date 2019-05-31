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

import com.netflix.titus.api.supervisor.model.MasterInstance;
import reactor.core.publisher.Flux;

/**
 * Each cluster/supervised member must resolve its own state and advertise it to other members.
 * This interface is implemented by components providing their own state resolution strategies.
 */
public interface LocalMasterInstanceResolver {

    /**
     * Emits the current member state, and state updates subsequently. The stream never completes, unless explicitly
     * terminated by a subscriber.
     */
    Flux<MasterInstance> observeLocalMasterInstanceUpdates();
}
