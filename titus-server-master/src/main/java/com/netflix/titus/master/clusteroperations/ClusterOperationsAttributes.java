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

package com.netflix.titus.master.clusteroperations;

/**
 * Collection of cluster operations related attributes that can be associated with agent instance groups and agent instances.
 */
public final class ClusterOperationsAttributes {

    /**
     * Mark an instance group or agent instance such that it will not get terminated by the cluster operations component.
     */
    public static final String NOT_REMOVABLE = "titus.clusterOperations.notRemovable";

    /**
     * Mark an agent instance such that it will get terminated. Note that the cluster operations component
     * will mark an agent instance once it determines it can be terminated.
     */
    public static final String REMOVABLE = "titus.clusterOperations.removable";
}
