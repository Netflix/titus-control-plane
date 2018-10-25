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

package com.netflix.titus.testkit.model.eviction;

import com.netflix.titus.api.eviction.service.EvictionOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.testkit.model.job.JobComponentStub;

public class EvictionComponentStub {

    private final StubbedEvictionData stubbedEvictionData;
    private final EvictionOperations evictionOperations;
    private final StubbedEvictionServiceClient evictionServiceClient;

    public EvictionComponentStub(JobComponentStub jobComponentStub, TitusRuntime titusRuntime) {
        this.stubbedEvictionData = new StubbedEvictionData();
        this.evictionOperations = new StubbedEvictionOperations(stubbedEvictionData, jobComponentStub.getJobOperations());
        this.evictionServiceClient = new StubbedEvictionServiceClient(evictionOperations);
    }

    public EvictionOperations getEvictionOperations() {
        return evictionOperations;
    }

    public void setQuota(String jobId, int quota) {
        stubbedEvictionData.setQuota(jobId, quota);
    }

    public EvictionServiceClient getEvictionServiceClient() {
        return evictionServiceClient;
    }
}
