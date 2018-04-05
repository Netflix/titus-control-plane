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
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import com.netflix.titus.master.endpoint.common.TaskSummary;
import com.netflix.titus.master.endpoint.v2.rest.representation.JobKillCmd;
import com.netflix.titus.master.endpoint.v2.rest.representation.JobSetInServiceCmd;
import com.netflix.titus.master.endpoint.v2.rest.representation.JobSetInstanceCountsCmd;
import com.netflix.titus.master.endpoint.v2.rest.representation.TaskKillCmd;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

/**
 * REST API for job and task management.
 */
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface JobManagementEndpoint {

    @GET
    @Path("/api/v2/jobs")
    List<TitusJobInfo> getJobs(
            @QueryParam("taskState") List<String> taskStates,
            @QueryParam("labels") List<String> labels,
            @QueryParam("labels.op") String labelsOp,
            @QueryParam("applicationName") String imageName,
            @QueryParam("appName") String appName,
            @QueryParam("type") String type,
            @QueryParam("limit") int limit,
            @QueryParam("jobGroupStack") String jobGroupStack,
            @QueryParam("jobGroupDetail") String jobGroupDetail,
            @QueryParam("jobGroupSequence") String jobGroupSequence
    );

    @GET
    @Path("/api/v2/jobs/{jobId}")
    TitusJobInfo getJob(@PathParam("jobId") String jobId, @QueryParam("taskState") List<String> taskStates);

    @POST
    @Path("/api/v2/jobs")
    Response addJob(TitusJobSpec jobSpec);

    @POST
    @Path("/api/v2/jobs/setinstancecounts")
    Response setInstanceCount(JobSetInstanceCountsCmd setInstanceCountsCmd);

    @POST
    @Path("/api/v2/jobs/setinservice")
    Response setInServiceCmd(JobSetInServiceCmd setInServiceCmd);

    @POST
    @Path("/api/v2/jobs/kill")
    Response killJob(JobKillCmd jobKillCmd);

    @GET
    @Path("/api/v2/tasks/{taskId}")
    TitusTaskInfo getTask(@PathParam("taskId") String taskId);

    @POST
    @Path("/api/v2/tasks/kill")
    Response killTask(TaskKillCmd taskKillCmd);

    @GET
    @Path("/api/v2/tasks/summary")
    List<TaskSummary> getTaskSummary();
}
