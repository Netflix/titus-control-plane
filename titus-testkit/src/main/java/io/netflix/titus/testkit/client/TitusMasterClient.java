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

package io.netflix.titus.testkit.client;

import io.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.master.endpoint.v2.rest.representation.JobSetInstanceCountsCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import rx.Observable;

/**
 * A simple implementation of Titus v2 REST API, which reuses server side REST API data model.
 * It is provided solely for testing purposes. This implementation is provided as a default client by
 * {@link TitusMasterResource}.
 */
public interface TitusMasterClient {

    /*
     * Job management.
     */

    Observable<String> submitJob(TitusJobSpec jobSpec);

    Observable<TitusJobInfo> findJobs(TitusTaskState... taskStates);

    Observable<TitusJobInfo> findAllJobs();

    Observable<TitusJobInfo> findJob(String jobId, boolean includeArchived);

    Observable<TitusJobInfo> findJob(String jobId, TitusTaskState... taskStates);

    Observable<TitusTaskInfo> findTask(String taskId, boolean includeArchived);

    Observable<Void> killJob(String jobId);

    Observable<Void> killTask(String taskId);

    Observable<Void> killTaskAndShrink(String taskId);

    Observable<Void> setInstanceCount(JobSetInstanceCountsCmd cmd);

    /*
     * Capacity group management.
     */

    Observable<String> addApplicationSLA(ApplicationSlaRepresentation applicationSLA);

    Observable<ApplicationSlaRepresentation> findAllApplicationSLA();

    Observable<ApplicationSlaRepresentation> findApplicationSLA(String name);

    Observable<Void> updateApplicationSLA(ApplicationSlaRepresentation applicationSLA);

    Observable<Void> deleteApplicationSLA(String name);
}
