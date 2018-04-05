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

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.titus.master.endpoint.common.QueueSummary;

/**
 * REST API for internal usage.
 */
@Path(SchedulerEndpoint.PATH_API_V2_SCHEDULER)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface SchedulerEndpoint {

    String PATH_API_V2_SCHEDULER = "/api/v2/scheduler";

    String PATH_LIST_QUEUE = "listq";

    String PATH_LIST_TASK_FAILURES = "failures";

    String PATH_QUEUE_SUMMARY = "qsummary";

    @GET
    @Path(PATH_LIST_QUEUE)
    Map<TaskQueue.TaskState, Object> getQueues(@QueryParam("state") List<String> stateNames);

    @GET
    @Path(PATH_LIST_TASK_FAILURES + "/{taskId}")
    Map<String, Object> getTaskFailures(@PathParam("taskId") String taskId);

    @GET
    @Path(PATH_QUEUE_SUMMARY)
    Map<String, SortedMap<String, QueueSummary>> getQueueSummary();
}
