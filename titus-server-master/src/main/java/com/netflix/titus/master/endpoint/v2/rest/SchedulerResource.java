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

package com.netflix.titus.master.endpoint.v2.rest;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.titus.master.endpoint.common.QueueSummary;
import com.netflix.titus.master.endpoint.common.SchedulerUtil;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.master.scheduler.SimpleFailuresAnalyzer;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.endpoint.common.QueueSummary;
import com.netflix.titus.master.endpoint.common.SchedulerUtil;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.master.scheduler.SimpleFailuresAnalyzer;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.swagger.annotations.Api;

@Api(tags = "Scheduler")
@Path(SchedulerResource.PATH_API_V2_SCHEDULER)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class SchedulerResource implements SchedulerEndpoint {

    private final static int MAX_IDS_LIST_SIZE = 20;

    private final SchedulingService schedulingService;
    private final ApplicationSlaManagementService applicationSlaManagementService;

    @Inject
    public SchedulerResource(SchedulingService schedulingService,
                             ApplicationSlaManagementService applicationSlaManagementService) {
        this.schedulingService = schedulingService;
        this.applicationSlaManagementService = applicationSlaManagementService;
    }

    @GET
    @Path(PATH_LIST_QUEUE)
    @Override
    public Map<TaskQueue.TaskState, Object> getQueues(@QueryParam("state") List<String> stateNames) {
        Set<TaskQueue.TaskState> states = valuesOf(stateNames);

        final Map<TaskQueue.TaskState, Collection<QueuableTask>> tasksMap =
                SchedulerUtil.blockAndGetTasksFromQueue(schedulingService);
        if (tasksMap == null) {
            throw new WebApplicationException(new TimeoutException("Timed out waiting for queue list"), Response.Status.INTERNAL_SERVER_ERROR);
        }

        final Map<TaskQueue.TaskState, Object> result = new HashMap<>();
        for (TaskQueue.TaskState s : states) {
            final Collection<QueuableTask> tasks = tasksMap.get(s);
            if (tasks != null && !tasks.isEmpty()) {
                for (QueuableTask t : tasks) {
                    final int tier = t.getQAttributes().getTierNumber();
                    final String name = t.getQAttributes().getBucketName();
                    final String id = t.getId();
                    result.putIfAbsent(s, new HashMap<>());
                    final Map<Integer, Map<String, List<String>>> stateMap = (Map<Integer, Map<String, List<String>>>) result.get(s);
                    stateMap.putIfAbsent(tier, new TreeMap<>()); // use TreeMap to sort the bucket names
                    final Map<String, List<String>> bucketsMap = stateMap.get(tier);
                    bucketsMap.putIfAbsent(name, new LinkedList<>());
                    final List<String> ids = bucketsMap.get(name);
                    if (ids.size() < MAX_IDS_LIST_SIZE) {
                        ids.add(id);
                    } else {
                        // After filling the bucket with enough ids, put the count for rest of them in a side-car bucket
                        // It's a hack, but, but good enough for debugging, which is the primary use case for this
                        final String suffixedName = name + "-NotShown";
                        bucketsMap.putIfAbsent(suffixedName, new LinkedList<>());
                        final List<String> idsCount = bucketsMap.get(suffixedName);
                        if (idsCount.isEmpty()) {
                            idsCount.add(Integer.toString(1));
                        } else {
                            int count = Integer.parseInt(idsCount.remove(0));
                            idsCount.add(Integer.toString(count + 1));
                        }

                    }
                }
            }
        }

        return result;
    }

    @GET
    @Path(PATH_LIST_TASK_FAILURES + "/{taskId}")
    @Override
    public Map<String, Object> getTaskFailures(@PathParam("taskId") String taskId) {
        if (!StringExt.isNotEmpty(taskId)) {
            throw new WebApplicationException(new IllegalArgumentException("Must provide taskId in request path"), Response.Status.BAD_REQUEST);
        }

        final List<TaskAssignmentResult> failures =
                SchedulerUtil.blockAndGetTaskAssignmentFailures(schedulingService, taskId);

        if (failures == null) {
            throw new WebApplicationException(
                    new IllegalArgumentException("No failures found for the task, or no agents available, or timed out getting results"),
                    Response.Status.NOT_FOUND
            );
        }
        if (failures.size() == 1 && failures.get(0) == null) {
            throw new WebApplicationException(
                    new IllegalArgumentException("Too many concurrent requests"),
                    Response.Status.SERVICE_UNAVAILABLE
            );
        }

        SimpleFailuresAnalyzer analyzer = new SimpleFailuresAnalyzer(taskId, failures);
        return analyzer.getAnalysisAsMapOfStrings();
    }

    @GET
    @Path(PATH_QUEUE_SUMMARY)
    @Override
    public Map<String, SortedMap<String, QueueSummary>> getQueueSummary() {
        final Map<TaskQueue.TaskState, Collection<QueuableTask>> tasksMap =
                SchedulerUtil.blockAndGetTasksFromQueue(schedulingService);
        if (tasksMap == null) {
            throw new WebApplicationException(new TimeoutException("Timed out waiting for queue list"), Response.Status.INTERNAL_SERVER_ERROR);
        }
        Map<String, SortedMap<String, QueueSummary>> result = new HashMap<>();
        QueueSummary total = new QueueSummary("TOTAL");
        for (TaskQueue.TaskState state : TaskQueue.TaskState.values()) {
            final Collection<QueuableTask> tasks = tasksMap.get(state);
            if (tasks != null && !tasks.isEmpty()) {
                for (QueuableTask t : tasks) {
                    final int tierNumber = t.getQAttributes().getTierNumber();
                    result.putIfAbsent(Integer.toString(tierNumber), new TreeMap<>());
                    final SortedMap<String, QueueSummary> queueSummarySortedMap = result.get(Integer.toString(tierNumber));
                    final String bucketName = t.getQAttributes().getBucketName();
                    queueSummarySortedMap.putIfAbsent(bucketName, new QueueSummary(bucketName));
                    final QueueSummary queueSummary = queueSummarySortedMap.get(bucketName);
                    if (!queueSummary.getGuaranteesSet()) {
                        ApplicationSLA applicationSLA = applicationSlaManagementService.getApplicationSLA(bucketName);
                        if (applicationSLA == null) {
                            applicationSlaManagementService.getApplicationSLA(ApplicationSlaManagementService.DEFAULT_APPLICATION);
                        }
                        queueSummary.setCapacityGuarantee(
                                applicationSLA == null ? null : applicationSLA.getResourceDimension(),
                                applicationSLA == null ? 0 : applicationSLA.getInstanceCount()
                        );
                    }
                    queueSummary.addTask(t, state);
                    total.addTask(t, state);
                }
            }
        }
        return result;
    }

    private static Set<TaskQueue.TaskState> valuesOf(List<String> states) {
        if (CollectionsExt.isNullOrEmpty(states)) {
            return Collections.singleton(TaskQueue.TaskState.QUEUED);
        }
        Set<TaskQueue.TaskState> result = new HashSet<>();
        for (String stateName : states) {
            try {
                result.add(TaskQueue.TaskState.valueOf(stateName));
            } catch (IllegalArgumentException ignore) {
            }
        }
        return result;
    }
}
