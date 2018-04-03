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

package com.netflix.titus.master.job;

import java.util.List;

import com.netflix.titus.api.model.v2.V2JobDefinition;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;

public interface V2JobMgrIntf extends JobMgr {

    V2JobDefinition getJobDefinition();

    V2JobMetadata getJobMetadata();

    V2JobMetadata getJobMetadata(boolean evenIfArchived);

    List<? extends V2WorkerMetadata> getWorkers();

    List<? extends V2WorkerMetadata> getArchivedWorkers();

    V2WorkerMetadata getTask(int workerNumber, boolean evenIfArchived);

}
