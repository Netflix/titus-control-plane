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

package com.netflix.titus.master.kubernetes.pod.taint;

import java.util.function.Function;

import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1Toleration;

public final class Tolerations {

    public static final V1Toleration TOLERATION_VIRTUAL_KUBLET = new V1Toleration()
            .key(KubeConstants.TAINT_VIRTUAL_KUBLET)
            .value("titus")
            .operator("Equal")
            .effect("NoSchedule");

    public static final V1Toleration TOLERATION_KUBE_SCHEDULER = new V1Toleration()
            .key(KubeConstants.TAINT_SCHEDULER)
            .value(KubeConstants.TAINT_SCHEDULER_VALUE_KUBE)
            .operator("Equal")
            .effect("NoSchedule");

    public static final V1Toleration TOLERATION_TIER_FLEX = new V1Toleration()
            .key(KubeConstants.TAINT_TIER)
            .value("flex")
            .operator("Equal")
            .effect("NoSchedule");

    public static final V1Toleration TOLERATION_TIER_CRITICAL = new V1Toleration()
            .key(KubeConstants.TAINT_TIER)
            .value("critical")
            .operator("Equal")
            .effect("NoSchedule");

    public static final Function<String, V1Toleration> TOLERATION_FARZONE_FACTORY = farzoneId -> new V1Toleration()
            .key(KubeConstants.TAINT_FARZONE)
            .value(farzoneId)
            .operator("Equal")
            .effect("NoSchedule");

    public static final V1Toleration TOLERATION_GPU_INSTANCE = new V1Toleration()
            .key(KubeConstants.TAINT_GPU_INSTANCE)
            .operator("Exists")
            .effect("NoSchedule");

    public static final V1Toleration TOLERATION_DECOMMISSIONING = new V1Toleration()
            .key(KubeConstants.TAINT_NODE_DECOMMISSIONING)
            .operator("Exists")
            .effect("NoSchedule");
}
