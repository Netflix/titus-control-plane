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

package com.netflix.titus.master.mesos.kubeapiserver.direct.taint;

import com.netflix.titus.master.mesos.kubeapiserver.direct.KubeConstants;
import io.kubernetes.client.models.V1Toleration;

public final class Tolerations {

    public static final V1Toleration TOLERATION_VIRTUAL_KUBLET = new V1Toleration()
            .key(KubeConstants.TAINT_VIRTUAL_KUBLET)
            .value("titus")
            .operator("Equal")
            .effect("NoSchedule");

    public static final V1Toleration TOLERATION_GPU_INSTANCE = new V1Toleration()
            .key(KubeConstants.TAINT_GPU_INSTANCE)
            .value("gpuWorkloads")
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
            .effect("NoSchedule");
}
