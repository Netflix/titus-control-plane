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

import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeConfiguration;

/**
 * Farzone resource pool resolver.
 */
public class FarzonePodResourcePoolResolver implements PodResourcePoolResolver {

    private final DirectKubeConfiguration configuration;

    public FarzonePodResourcePoolResolver(DirectKubeConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public List<ResourcePoolAssignment> resolve(Job<?> job) {
        return KubeUtil.findFarzoneId(configuration, job).map(farzone -> {
                    String resourcePoolName = PodResourcePoolResolvers.RESOURCE_POOL_ELASTIC_FARZONE_PREFIX + farzone;
                    return Collections.singletonList(ResourcePoolAssignment.newBuilder()
                            .withResourcePoolName(resourcePoolName)
                            .withRule(String.format("Farzone %s assigned to resource pool %s", farzone, resourcePoolName))
                            .build()
                    );
                }
        ).orElse(Collections.emptyList());
    }
}
