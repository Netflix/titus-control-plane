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

package io.netflix.titus.api.store.v2;

import java.net.URL;
import java.util.Collection;
import java.util.List;

import io.netflix.titus.api.audit.model.AuditLogEvent;
import io.netflix.titus.api.model.v2.JobSla;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.parameter.Parameter;

public interface V2JobMetadata {

    String getJobId();

    String getName();

    String getUser();

    long getSubmittedAt();

    URL getJarUrl();

    JobSla getSla();

    long getSubscriptionTimeoutSecs();

    V2JobState getState();

    List<Parameter> getParameters();

    Collection<? extends V2StageMetadata> getStageMetadata();

    int getNumStages();

    V2StageMetadata getStageMetadata(int stageNum);

    V2WorkerMetadata getWorkerByIndex(int stageNumber, int workerIndex) throws InvalidJobException;

    V2WorkerMetadata getWorkerByNumber(int workerNumber) throws InvalidJobException;

    AutoCloseable obtainLock();

    List<AuditLogEvent> getLatestAuditLogEvents();

    int getNextWorkerNumberToUse();

}
