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

package com.netflix.titus.master;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.netflix.titus.master.job.V2JobMgrIntf;
import com.netflix.titus.master.store.NamedJob;
import com.netflix.titus.master.store.V2JobStore;
import com.netflix.titus.api.store.v2.InvalidJobException;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.master.job.V2JobMgrIntf;
import com.netflix.titus.master.store.NamedJob;
import com.netflix.titus.master.store.V2JobStore;

public interface ApiOperations {

    Set<String> getAllJobIds();

    List<V2JobMetadata> getAllJobsMetadata(boolean activeOnly, int limit);

    V2JobMetadata getJobMetadata(String jobId);

    List<? extends V2WorkerMetadata> getArchivedWorkers(String jobId);

    V2WorkerMetadata getWorker(String taskInstanceId);

    V2WorkerMetadata getWorker(String jobId, int number, boolean anyState);

    ConcurrentMap<String, NamedJob> getNamedJobs();

    Set<String> getAllActiveJobs();

    List<V2WorkerMetadata> getRunningWorkers(String jobId);

    List<V2WorkerMetadata> getAllWorkers(String jobId);

    boolean killJob(String jobId, String user);

    boolean killWorker(String taskId, String user);

    boolean killWorkerAndShrink(String taskId, String user);

    void updateInstanceCounts(String jobId, int stageNum, int min, int desired, int max, String user) throws InvalidJobException;

    void updateJobProcesses(String jobId, int stageNum, boolean disableIncreaseDesired, boolean disableDecreaseDesired, String user) throws InvalidJobException;

    void updateInServiceStatus(String jobId, int stage, boolean inService, String user) throws InvalidJobException;

    void setReady(V2JobStore store);

    boolean isReady();

    void removeJobIdRef(String jobId);

    void addJobIdRef(String jobId, V2JobMgrIntf jobMgr);
}
