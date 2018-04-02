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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import com.netflix.titus.api.model.v2.V2JobDefinition;
import com.netflix.titus.master.store.NamedJob;
import com.netflix.titus.master.store.V2JobStore;
import rx.functions.Func2;
import rx.subjects.Subject;

public interface V2JobOperations {

    String submit(V2JobDefinition jobDefinition);

    boolean deleteJob(String jobId) throws IOException;

    void killJob(String user, String jobId, String reason);

    void terminateJob(String jobId);

    Func2<V2JobStore, Map<String, V2JobDefinition>, Collection<NamedJob>> getJobsInitializer();

    Collection<V2JobMgrIntf> getAllJobMgrs();

    V2JobMgrIntf getJobMgr(String jobId);

    V2JobMgrIntf getJobMgrFromTaskId(String taskId);

    Subject<V2JobMgrIntf, V2JobMgrIntf> getJobCreationPublishSubject();
}
