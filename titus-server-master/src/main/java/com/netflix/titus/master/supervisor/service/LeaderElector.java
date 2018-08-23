/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.supervisor.service;

import com.netflix.titus.master.supervisor.model.MasterState;
import rx.Observable;

/**
 * Leader elector interface.
 */
public interface LeaderElector {

    /**
     * Instruct the {@link LeaderElector} instance to join the leader election process.
     */
    boolean join();

    /**
     * Instruct the {@link LeaderElector} instance to leave the leader election process. If the current TitusMaster
     * is already the leader, does not do anything.
     */
    boolean leaveIfNotLeader();

    /**
     * Emits {@link MasterState#LeaderActivating} when the given instance of TitusMaster becomes the leader, and
     * starts the activation process. Emits {@link MasterState#LeaderActivated} when the activation process is
     * completed, and than completes. When subscription happens after the activation happened, the observable
     * emits {@link MasterState#LeaderActivated} immediately and completes.
     */
    Observable<MasterState> awaitElection();
}
