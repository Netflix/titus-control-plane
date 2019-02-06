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

package com.netflix.titus.supplementary.relocation.endpoint.rest;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.grpc.protogen.TaskRelocationExecution;
import com.netflix.titus.grpc.protogen.TaskRelocationExecutions;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.runtime.relocation.endpoint.RelocationGrpcModelConverters;
import com.netflix.titus.supplementary.relocation.endpoint.TaskRelocationPlanPredicate;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import com.netflix.titus.supplementary.relocation.workflow.RelocationWorkflowExecutor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static com.netflix.titus.supplementary.relocation.endpoint.TaskRelocationPlanPredicate.FILTER_APPLICATION_NAME;
import static com.netflix.titus.supplementary.relocation.endpoint.TaskRelocationPlanPredicate.FILTER_CAPACITY_GROUP;
import static com.netflix.titus.supplementary.relocation.endpoint.TaskRelocationPlanPredicate.FILTER_JOB_IDS;
import static com.netflix.titus.supplementary.relocation.endpoint.TaskRelocationPlanPredicate.FILTER_TASK_IDS;

@RestController
@RequestMapping(path = "/api/v3/relocation")
public class TaskRelocationSpringResource {

    private final ReadOnlyJobOperations jobOperations;
    private final RelocationWorkflowExecutor relocationWorkflowExecutor;
    private final TaskRelocationResultStore archiveStore;

    @Inject
    public TaskRelocationSpringResource(ReadOnlyJobOperations jobOperations,
                                        RelocationWorkflowExecutor relocationWorkflowExecutor,
                                        TaskRelocationResultStore archiveStore) {
        this.jobOperations = jobOperations;
        this.relocationWorkflowExecutor = relocationWorkflowExecutor;
        this.archiveStore = archiveStore;
    }

    @RequestMapping(method = RequestMethod.GET, path = "/plans", produces = "application/json")
    public TaskRelocationPlans getCurrentTaskRelocationPlans(@RequestParam(name = FILTER_JOB_IDS, required = false) String jobIds,
                                                             @RequestParam(name = FILTER_TASK_IDS, required = false) String taskIds,
                                                             @RequestParam(name = FILTER_APPLICATION_NAME, required = false) String applicationName,
                                                             @RequestParam(name = FILTER_CAPACITY_GROUP, required = false) String capacityGroup) {
        TaskRelocationQuery.Builder requestBuilder = TaskRelocationQuery.newBuilder();
        Evaluators.acceptNotNull(jobIds, value -> requestBuilder.putFilteringCriteria(FILTER_JOB_IDS, value));
        Evaluators.acceptNotNull(taskIds, value -> requestBuilder.putFilteringCriteria(FILTER_TASK_IDS, value));
        Evaluators.acceptNotNull(applicationName, value -> requestBuilder.putFilteringCriteria(FILTER_APPLICATION_NAME, value));
        Evaluators.acceptNotNull(capacityGroup, value -> requestBuilder.putFilteringCriteria(FILTER_CAPACITY_GROUP, value));

        return TaskRelocationPlanPredicate.buildProtobufQueryResult(jobOperations, relocationWorkflowExecutor, requestBuilder.build());
    }

    @RequestMapping(method = RequestMethod.GET, path = "/plans/{taskId}", produces = "application/json")
    public com.netflix.titus.grpc.protogen.TaskRelocationPlan getTaskRelocationPlan(@PathVariable("taskId") String taskId) {
        TaskRelocationPlan plan = relocationWorkflowExecutor.getPlannedRelocations().get(taskId);
        if (plan != null) {
            return RelocationGrpcModelConverters.toGrpcTaskRelocationPlan(plan);
        }
        throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
    }

    @RequestMapping(method = RequestMethod.GET, path = "/executions", produces = "application/json")
    public TaskRelocationExecutions getTaskRelocationResults() {
        List<TaskRelocationStatus> coreResults = new ArrayList<>(relocationWorkflowExecutor.getLastEvictionResults().values());
        return RelocationGrpcModelConverters.toGrpcTaskRelocationExecutions(coreResults);
    }

    @RequestMapping(method = RequestMethod.GET, path = "/executions/{taskId}", produces = "application/json")
    public TaskRelocationExecution getTaskRelocationResult(@PathVariable("taskId") String taskId) {
        TaskRelocationStatus latest = relocationWorkflowExecutor.getLastEvictionResults().get(taskId);
        List<TaskRelocationStatus> archived = archiveStore.getTaskRelocationStatusList(taskId).block();

        if (latest == null && archived.isEmpty()) {
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        }

        List<TaskRelocationStatus> combined;
        if (latest == null) {
            combined = archived;
        } else if (archived.isEmpty()) {
            combined = Collections.singletonList(latest);
        } else {
            if (CollectionsExt.last(archived).equals(latest)) {
                combined = archived;
            } else {
                combined = CollectionsExt.copyAndAdd(archived, latest);
            }
        }

        return RelocationGrpcModelConverters.toGrpcTaskRelocationExecution(combined);
    }
}
