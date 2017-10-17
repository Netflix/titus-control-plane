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

package io.netflix.titus.master.endpoint.v2.rest;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import io.netflix.titus.api.endpoint.v2.rest.representation.JobSubmitReply;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.api.model.Pagination;
import io.netflix.titus.api.model.event.UserRequestEvent;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.common.util.tuple.Either;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.ApiOperations;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.endpoint.common.TaskSummary;
import io.netflix.titus.master.endpoint.v2.V2LegacyTitusServiceGateway;
import io.netflix.titus.master.endpoint.v2.rest.caller.CallerIdResolver;
import io.netflix.titus.master.endpoint.v2.rest.representation.JobKillCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.JobSetInServiceCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.JobSetInstanceCountsCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.TaskKillCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.TaskKillCmdError;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.master.endpoint.v2.validator.TitusJobSpecValidators;
import io.netflix.titus.master.endpoint.v2.validator.ValidatorConfiguration;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;
import io.netflix.titus.runtime.endpoint.common.rest.Responses;
import io.netflix.titus.runtime.endpoint.common.rest.RestException;
import io.swagger.annotations.Api;
import rx.Notification;
import rx.Observable;

import static io.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;
import static io.netflix.titus.common.util.StringExt.trimAndApplyIfNonEmpty;

@Api(tags = "Job")
@Path("/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class JobManagementResource implements JobManagementEndpoint {

    private final V2LegacyTitusServiceGateway legacyTitusServiceGateway;
    private final MasterConfiguration configuration;
    private final TitusJobSpecValidators titusJobSpecValidators;
    private final ApiOperations apiOperations;
    private final CallerIdResolver callerIdResolver;
    private final RxEventBus eventBus;

    @Context
    private HttpServletRequest httpServletRequest;

    @Inject
    public JobManagementResource(V2LegacyTitusServiceGateway legacyTitusServiceGateway,
                                 MasterConfiguration configuration,
                                 ValidatorConfiguration validatorConfiguration,
                                 ApiOperations apiOperations,
                                 CallerIdResolver callerIdResolver,
                                 RxEventBus eventBus) {
        this.legacyTitusServiceGateway = legacyTitusServiceGateway;
        this.configuration = configuration;
        this.titusJobSpecValidators = new TitusJobSpecValidators(configuration, validatorConfiguration);
        this.apiOperations = apiOperations;
        this.callerIdResolver = callerIdResolver;
        this.eventBus = eventBus;
    }

    @GET
    @Path("/api/v2/jobs")
    public List<TitusJobInfo> getJobs(@QueryParam("taskState") List<String> taskStates,
                                      @QueryParam("labels") List<String> labels,
                                      @QueryParam("labels.op") String labelsOp,
                                      @QueryParam("applicationName") String imageName,
                                      @QueryParam("appName") String appName,
                                      @QueryParam("type") String type,
                                      @QueryParam("limit") int limit,
                                      @QueryParam("jobGroupStack") String jobGroupStack,
                                      @QueryParam("jobGroupDetail") String jobGroupDetail,
                                      @QueryParam("jobGroupSequence") String jobGroupSequence) {
        JobQueryCriteria.Builder<TitusTaskState, TitusJobType> criteriaBuilder = JobQueryCriteria.newBuilder();

        criteriaBuilder.withImageName(imageName);
        criteriaBuilder.withAppName(appName);

        // taskState
        Set<TitusTaskState> taskStateSet = new HashSet<>(QueryParametersUtil.getTaskStatesFromParams(taskStates));
        criteriaBuilder.withTaskStates(taskStateSet);
        criteriaBuilder.withIncludeArchived(QueryParametersUtil.hasArchivedState(taskStateSet));

        // labels
        Map<String, String> parsedLabels = QueryParametersUtil.buildLabelMap(labels);
        criteriaBuilder.withLabels(parsedLabels.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, entry -> Collections.singleton(entry.getValue())))
        );
        criteriaBuilder.withLabelsAndOp("and".equals(labelsOp));

        // type
        String trimmedType = StringExt.safeTrim(type);
        if (!trimmedType.isEmpty()) {
            try {
                criteriaBuilder.withJobType(TitusJobType.valueOf(trimmedType));
            } catch (Exception ignore) {
            }
        }

        // Job cluster
        trimAndApplyIfNonEmpty(StringExt.safeTrim(jobGroupStack), criteriaBuilder::withJobGroupStack);
        trimAndApplyIfNonEmpty(StringExt.safeTrim(jobGroupDetail), criteriaBuilder::withJobGroupDetail);
        trimAndApplyIfNonEmpty(StringExt.safeTrim(jobGroupSequence), criteriaBuilder::withJobGroupSequence);

        // Limit
        criteriaBuilder.withLimit(limit);

        JobQueryCriteria<TitusTaskState, TitusJobType> criteria = criteriaBuilder.build();
        if (criteria.isEmpty()) {
            if (isNullOrEmpty(taskStates)) {
                throw new WebApplicationException(new IllegalArgumentException("taskState query parameter not defined"), Status.BAD_REQUEST);
            }
            throw new WebApplicationException(new IllegalArgumentException("no valid taskState query parameter defined: " + taskStates), Status.BAD_REQUEST);
        }

        Pair<List<TitusJobInfo>, Pagination> queryResult = legacyTitusServiceGateway.findJobsByCriteria(criteria, Optional.empty());
        return queryResult.getLeft();
    }

    @GET
    @Path("/api/v2/jobs/{jobId}")
    public TitusJobInfo getJob(@PathParam("jobId") String jobId, @QueryParam("taskState") List<String> taskStates) {
        HashSet<TitusTaskState> taskStateSet = new HashSet<>(QueryParametersUtil.getTaskStatesFromParams(taskStates));
        boolean includeArchived = QueryParametersUtil.hasArchivedState(taskStateSet);
        return Responses.fromSingleValueObservable(legacyTitusServiceGateway.findJobById(jobId, includeArchived, taskStateSet));
    }

    @POST
    @Path("/api/v2/jobs")
    public Response addJob(TitusJobSpec jobSpec) {
        try {
            TitusJobSpec sanitizedJobSpec = TitusJobSpec.sanitize(configuration, jobSpec);

            final TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(sanitizedJobSpec);
            if (!validationResult.isValid) {
                throw RestException.newBuilder(Status.BAD_REQUEST.getStatusCode(),
                        String.format("Validation failed for JobSpec %s - Validation Result %s", jobSpec, validationResult.failedValidators))
                        .build();
            }

            String jobId = Responses.fromSingleValueObservable(legacyTitusServiceGateway.createJob(sanitizedJobSpec));
            String relativeURI = "/api/v2/jobs/" + jobId;

            eventBus.publish(new UserRequestEvent("POST /api/v2/jobs", resolveCallerId(), "jobId=" + jobId, System.currentTimeMillis()));

            return Response.ok().entity(new JobSubmitReply(relativeURI)).location(URI.create(relativeURI)).build();
        } catch (Exception ex) {
            eventBus.publish(new UserRequestEvent("POST /api/v2/jobs", resolveCallerId(), "ERROR: " + ex.getMessage(), System.currentTimeMillis()));
            throw ex;
        }
    }

    private String resolveCallerId() {
        return callerIdResolver.resolve(httpServletRequest).orElse("UNKNOWN");
    }

    @POST
    @Path("/api/v2/jobs/setinstancecounts")
    public Response setInstanceCount(JobSetInstanceCountsCmd cmd) {
        try {
            int min = cmd.getInstancesMin();
            int desired = cmd.getInstancesDesired();
            int max = cmd.getInstancesMax();

            if (min < 0) {
                throw new WebApplicationException(new IllegalArgumentException("Instance count min < 0 not allowed: " + min), Status.BAD_REQUEST);
            }
            if (desired < min) {
                throw new WebApplicationException(new IllegalArgumentException("Desired instance count must be greater or equal min: " + desired), Status.BAD_REQUEST);
            }
            if (max < desired) {
                throw new WebApplicationException(new IllegalArgumentException("Max instance count must be greater or equal desired: " + max), Status.BAD_REQUEST);
            }
            if (max > configuration.getMaxServiceInstances()) {
                throw new WebApplicationException(new IllegalArgumentException("Max instance count must be <= " + configuration.getMaxServiceInstances()), Status.BAD_REQUEST);
            }

            eventBus.publish(new UserRequestEvent("POST /api/v2/jobs/setinstancecounts", resolveCallerId(), "cmd=" + cmd, System.currentTimeMillis()));

            return Responses.fromVoidObservable(legacyTitusServiceGateway.resizeJob(cmd.getUser(), cmd.getJobId(), desired, min, max), Status.NO_CONTENT);
        } catch (Exception ex) {
            eventBus.publish(new UserRequestEvent("POST /api/v2/jobs/setinstancecounts", resolveCallerId(), "ERROR: " + ex.getMessage(), System.currentTimeMillis()));
            throw ex;
        }
    }

    @POST
    @Path("/api/v2/jobs/setinservice")
    public Response setInServiceCmd(JobSetInServiceCmd cmd) {
        try {
            Response response = Responses.fromVoidObservable(legacyTitusServiceGateway.changeJobInServiceStatus(cmd.getUser(), cmd.getJobId(), cmd.isInService()), Status.NO_CONTENT);
            eventBus.publish(new UserRequestEvent("POST /api/v2/jobs/setinservice", resolveCallerId(), "cmd=" + cmd, System.currentTimeMillis()));
            return response;
        } catch (Exception ex) {
            eventBus.publish(new UserRequestEvent("POST /api/v2/jobs/setinservice", resolveCallerId(), "ERROR: " + ex.getMessage(), System.currentTimeMillis()));
            throw ex;
        }
    }

    @POST
    @Path("/api/v2/jobs/kill")
    public Response killJob(JobKillCmd cmd) {
        try {
            Response response = Responses.fromVoidObservable(legacyTitusServiceGateway.killJob(cmd.getUser(), cmd.getJobId()), Status.NO_CONTENT);
            eventBus.publish(new UserRequestEvent("POST /api/v2/jobs/kill", resolveCallerId(), "cmd=" + cmd, System.currentTimeMillis()));
            return response;
        } catch (Exception ex) {
            eventBus.publish(new UserRequestEvent("POST /api/v2/jobs/kill", resolveCallerId(), "ERROR: " + ex.getMessage(), System.currentTimeMillis()));
            throw ex;
        }
    }

    @GET
    @Path("/api/v2/tasks/{taskId}")
    public TitusTaskInfo getTask(@PathParam("taskId") String taskId) {
        return Responses.fromSingleValueObservable(legacyTitusServiceGateway.findTaskById(taskId));
    }

    @POST
    @Path("/api/v2/tasks/kill")
    public Response killTask(TaskKillCmd cmd) {
        try {
            boolean isSingle = StringExt.isNotEmpty(cmd.getTaskId());
            boolean isMultiple = !CollectionsExt.isNullOrEmpty(cmd.getTaskIds());

            if (isSingle && isMultiple) {
                throw new WebApplicationException(new IllegalArgumentException("Both taskId and taskIds fields defined in the task kill command"), Status.BAD_REQUEST);
            }
            if (!isSingle && !isMultiple) {
                throw new WebApplicationException(new IllegalArgumentException("Both taskId and taskIds fields defined in the task kill command"), Status.BAD_REQUEST);
            }

            Set<String> allTasks = isSingle ? Collections.singleton(cmd.getTaskId()) : new HashSet<>(cmd.getTaskIds());

            if (cmd.isStrict()) {
                Set<String> aliveTasks = new HashSet<>(findAliveTaskIds(allTasks).toBlocking().first());

                if (aliveTasks.size() != allTasks.size()) {
                    Set<String> invalid = CollectionsExt.copyAndRemove(allTasks, aliveTasks);
                    throw new WebApplicationException(new IllegalArgumentException("Invalid task ids: " + invalid), Status.BAD_REQUEST);
                }
            }
            Pair<List<String>, List<Pair<String, Throwable>>> result = killTasks(cmd.getUser(), allTasks, cmd.isShrink(), cmd.isStrict())
                    .take(1)
                    .toBlocking().first();

            if (result.getRight().isEmpty()) {
                eventBus.publish(new UserRequestEvent("POST /api/v2/tasks/kill", resolveCallerId(), "cmd=" + cmd, System.currentTimeMillis()));
                return Response.noContent().build();
            }
            Throwable firstError = result.getRight().get(0).getRight();
            if (isSingle && result.getRight().size() == 1 && firstError instanceof RuntimeException) {
                throw (RuntimeException) firstError;
            }
            TaskKillCmdError details = buildFailedTaskReply(result);
            throw RestException.newBuilder(Status.INTERNAL_SERVER_ERROR.getStatusCode(), "failed to terminate tasks " + details.getFailed().keySet())
                    .withDetails(details)
                    .build();
        } catch (Exception ex) {
            eventBus.publish(new UserRequestEvent("POST /api/v2/tasks/kill", resolveCallerId(), "ERROR: " + ex.getMessage(), System.currentTimeMillis()));
            throw ex;
        }
    }

    @GET
    @Path("/api/v2/tasks/summary")
    public List<TaskSummary> getTaskSummary() {
        return Responses.fromSingleValueObservable(legacyTitusServiceGateway.getTaskSummary());
    }

    private Observable<List<String>> findAliveTaskIds(Set<String> taskIds) {
        List<Observable<TitusTaskInfo>> findActiveTasksObservable = taskIds.stream()
                .map(tid -> legacyTitusServiceGateway.findTaskById(tid).filter(t -> t.getState().isActive()).onErrorResumeNext(Observable.empty()))
                .collect(Collectors.toList());
        return Observable.merge(findActiveTasksObservable).map(TitusTaskInfo::getId).toList();
    }

    /**
     * Kill task, and return task id if operation succeeds or (taskId, exception) if it fails.
     */
    private Observable<Either<String, Pair<String, Throwable>>> killTask(String user, String taskId, boolean shrink, boolean strict) {
        return legacyTitusServiceGateway.killTask(user, taskId, shrink)
                .materialize()
                .map(notification -> {
                    if (notification.getKind() == Notification.Kind.OnError) {
                        if (!strict && notification.getThrowable() instanceof TitusServiceException) {
                            TitusServiceException ge = (TitusServiceException) notification.getThrowable();
                            if (ge.getErrorCode() == TitusServiceException.ErrorCode.TASK_NOT_FOUND) {
                                return Either.ofValue(taskId);
                            }
                        }
                        return Either.ofError(Pair.of(taskId, notification.getThrowable()));
                    }
                    return Either.ofValue(taskId);
                });
    }

    /**
     * Kill a collection of tasks, and return an aggregated result (see {@link #killTask(String, String, boolean, boolean)}).
     */
    private Observable<Pair<List<String>, List<Pair<String, Throwable>>>> killTasks(String user, Set<String> taskIds, boolean shrink, boolean strict) {
        List<Observable<Either<String, Pair<String, Throwable>>>> killTasksObservable = taskIds.stream()
                .map(tid -> killTask(user, tid, shrink, strict))
                .collect(Collectors.toList());

        return Observable.merge(killTasksObservable).reduce(
                Pair.of(new ArrayList<>(), new ArrayList<>()),
                (acc, next) -> {
                    if (next.hasValue()) {
                        acc.getLeft().add(next.getValue());
                    } else {
                        acc.getRight().add(next.getError());
                    }
                    return acc;
                }
        );
    }

    private TaskKillCmdError buildFailedTaskReply(Pair<List<String>, List<Pair<String, Throwable>>> result) {
        Map<String, String> errors = new HashMap<>();
        for (Pair<String, Throwable> ep : result.getRight()) {
            errors.put(ep.getLeft(), ep.getRight().getMessage());
        }
        return new TaskKillCmdError(result.getLeft(), errors);

    }
}
