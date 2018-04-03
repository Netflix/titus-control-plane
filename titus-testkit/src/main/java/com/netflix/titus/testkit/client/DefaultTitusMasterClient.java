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

package com.netflix.titus.testkit.client;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.api.endpoint.v2.rest.representation.JobSubmitReply;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import com.netflix.titus.common.network.client.RxRestClient;
import com.netflix.titus.common.network.client.RxRestClient.TypeProvider;
import com.netflix.titus.common.network.client.RxRestClients;
import com.netflix.titus.common.network.client.TypeProviders;
import com.netflix.titus.master.endpoint.v2.rest.representation.JobKillCmd;
import com.netflix.titus.master.endpoint.v2.rest.representation.JobSetInstanceCountsCmd;
import com.netflix.titus.master.endpoint.v2.rest.representation.TaskKillCmd;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import com.netflix.titus.runtime.endpoint.common.rest.ErrorResponse;
import rx.Observable;

import static com.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;

/**
 * {@link RxRestClient} based {@link TitusMasterClient} implementation.
 */
public class DefaultTitusMasterClient implements TitusMasterClient {

    private static final String USER_ID = "testClient";

    private static final TypeProvider<ErrorResponse> ERROR_RESPONSE_TP = TypeProviders.of(ErrorResponse.class);

    private static final TypeReference<List<TitusJobInfo>> LIST_OF_TITUS_JOB_INFO_TYPE = new TypeReference<List<TitusJobInfo>>() {
    };

    private static final TypeProvider<TitusJobInfo> TITUS_JOB_INFO_TP = TypeProviders.of(TitusJobInfo.class);

    private static final TypeProvider<List<TitusJobInfo>> LIST_OF_TITUS_JOB_INFO_TP = TypeProviders.of(LIST_OF_TITUS_JOB_INFO_TYPE);

    private final RxRestClient restClient;

    public DefaultTitusMasterClient(String hostName, int port) {
        this.restClient = RxRestClients.newBuilder("perfClient", new DefaultRegistry())
                .host(hostName)
                .port(port)
                .errorReplyTypeResolver(e -> ERROR_RESPONSE_TP)
                .build();
    }

    @Override
    public Observable<String> submitJob(TitusJobSpec jobSpec) {
        return restClient.doPOST("/api/v2/jobs", jobSpec, TypeProviders.of(JobSubmitReply.class))
                .map(jobRef -> jobRef.getJobUri().substring(jobRef.getJobUri().lastIndexOf('/') + 1));
    }

    @Override
    public Observable<TitusJobInfo> findAllJobs() {
        return findJobsInternal();
    }

    @Override
    public Observable<TitusJobInfo> findJobs(TitusTaskState... taskStates) {
        return findJobsInternal(taskStates);
    }

    @Override
    public Observable<TitusJobInfo> findJob(String jobId, boolean includeArchived) {
        return findJobInternal(jobId, null, includeArchived);
    }

    @Override
    public Observable<TitusJobInfo> findJob(String jobId, TitusTaskState... taskStates) {
        return findJobInternal(jobId, taskStates, false);
    }

    private Observable<TitusJobInfo> findJobsInternal(TitusTaskState... taskStates) {
        String path = "/api/v2/jobs";
        if (isNullOrEmpty(taskStates)) {
            path += "?taskState=ANY";
        } else {
            path += '?' + buildTaskStateQueryParam(taskStates);
        }
        return restClient.doGET(path, LIST_OF_TITUS_JOB_INFO_TP).flatMap(Observable::from);
    }

    private Observable<TitusJobInfo> findJobInternal(String jobId, TitusTaskState[] taskStates, boolean includeArchived) {
        String path = "/api/v2/jobs/" + jobId;
        if (isNullOrEmpty(taskStates)) {
            if (includeArchived) {
                path += "?taskState=ANY";
            }
        } else {
            path += '?' + buildTaskStateQueryParam(taskStates);
        }
        return restClient.doGET(path, TITUS_JOB_INFO_TP);
    }

    private <T> String buildTaskStateQueryParam(T[] taskStates) {
        StringBuilder sb = new StringBuilder();
        for (T taskState : taskStates) {
            sb.append("taskState=").append(taskState).append('&');
        }
        return sb.toString();
    }

    @Override
    public Observable<TitusTaskInfo> findTask(String taskId, boolean includeArchived) {
        String path = "/api/v2/tasks/" + taskId;
        if (includeArchived) {
            path += "?taskState=ANY";
        }
        return restClient.doGET(path, TypeProviders.of(TitusTaskInfo.class));
    }

    @Override
    public Observable<Void> killJob(String jobId) {
        return restClient.doPOST("/api/v2/jobs/kill", new JobKillCmd(USER_ID, jobId));
    }

    @Override
    public Observable<Void> killTask(String taskId) {
        return killTaskInternal(taskId, false);
    }

    @Override
    public Observable<Void> killTaskAndShrink(String taskId) {
        return killTaskInternal(taskId, true);
    }

    private Observable<Void> killTaskInternal(String taskId, boolean shrink) {
        TaskKillCmd killCmd = new TaskKillCmd(USER_ID, taskId, null, shrink, false);
        return restClient.doPOST("/api/v2/tasks/kill", killCmd);
    }

    @Override
    public Observable<Void> setInstanceCount(JobSetInstanceCountsCmd cmd) {
        return restClient.doPOST("/api/v2/jobs/setinstancecounts", cmd);
    }

    @Override
    public Observable<String> addApplicationSLA(ApplicationSlaRepresentation applicationSLA) {
        return restClient.doPOST(
                "/api/v2/management/applications", applicationSLA, TypeProviders.ofEmptyResponse()
        ).flatMap(response -> {
            if (response.getStatusCode() != 201) {
                return Observable.error(new IOException("Errored with HTTP status code " + response.getStatusCode()));
            }
            List<String> locationHeader = response.getHeaders().get("Location");
            if (locationHeader == null) {
                return Observable.error(new IOException("Location header not found in response"));
            }
            return Observable.just(locationHeader.get(0));
        });
    }

    @Override
    public Observable<ApplicationSlaRepresentation> findAllApplicationSLA() {
        return null;
    }

    @Override
    public Observable<ApplicationSlaRepresentation> findApplicationSLA(String name) {
        return null;
    }

    @Override
    public Observable<Void> updateApplicationSLA(ApplicationSlaRepresentation applicationSLA) {
        return null;
    }

    @Override
    public Observable<Void> deleteApplicationSLA(String name) {
        return null;
    }
}
