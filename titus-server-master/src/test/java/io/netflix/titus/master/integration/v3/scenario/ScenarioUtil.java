/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.integration.v3.scenario;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;

import static io.netflix.titus.testkit.junit.master.TitusStackResource.V2_ENGINE_APP_PREFIX;
import static io.netflix.titus.testkit.junit.master.TitusStackResource.V3_ENGINE_APP_PREFIX;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.serviceJobDescriptors;

public class ScenarioUtil {
    public static JobDescriptor.Builder<BatchJobExt> baseBatchJobDescriptor(boolean v2Mode) {
        String appName = v2Mode ? V2_ENGINE_APP_PREFIX : V3_ENGINE_APP_PREFIX;
        return batchJobDescriptors().getValue().toBuilder().withApplicationName(appName);
    }

    public static JobDescriptor.Builder<ServiceJobExt> baseServiceJobDescriptor(boolean v2Mode) {
        String appName = v2Mode ? V2_ENGINE_APP_PREFIX : V3_ENGINE_APP_PREFIX;
        return serviceJobDescriptors().getValue().toBuilder().withApplicationName(appName);
    }
}
