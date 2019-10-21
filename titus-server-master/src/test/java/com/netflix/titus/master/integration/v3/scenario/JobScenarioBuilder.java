/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.integration.v3.scenario;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.protobuf.Empty;
import com.google.protobuf.UInt32Value;
import com.netflix.fenzo.TaskRequest;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdateWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobCapacityWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.NotificationCase;
import com.netflix.titus.grpc.protogen.JobDisruptionBudgetUpdate;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.observables.ConnectableObservable;
import rx.subjects.ReplaySubject;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.common.util.ExceptionExt.rethrow;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.discoverActiveTest;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toCoreJob;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcCapacity;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcDisruptionBudget;
import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class JobScenarioBuilder {

    private static final Logger logger = LoggerFactory.getLogger(JobScenarioBuilder.class);

    private static final long TIMEOUT_MS = 30_000;

    private final EmbeddedTitusOperations titusOperations;
    private final JobsScenarioBuilder jobsScenarioBuilder;
    private final String jobId;
    private final SchedulingService<? extends TaskRequest> schedulingService;
    private final DiagnosticReporter diagnosticReporter;

    private final JobManagementServiceGrpc.JobManagementServiceStub client;

    private final ExtTestSubscriber<Job> jobEventStream = new ExtTestSubscriber<>();

    private volatile int nextIndex = 0;
    private final Multimap<Integer, String> taskSlotIndexes = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());
    private final Map<String, Integer> taskToSlot = new ConcurrentHashMap<>();
    private final Map<String, TaskHolder> taskHolders = new ConcurrentHashMap<>();

    private final Subscription eventStreamSubscription;

    public JobScenarioBuilder(EmbeddedTitusOperations titusOperations,
                              JobsScenarioBuilder jobsScenarioBuilder,
                              String jobId,
                              SchedulingService<? extends TaskRequest> schedulingService,
                              DiagnosticReporter diagnosticReporter) {
        this.client = titusOperations.getV3GrpcClient();
        this.titusOperations = titusOperations;
        this.jobsScenarioBuilder = jobsScenarioBuilder;
        this.jobId = jobId;
        this.schedulingService = schedulingService;
        this.diagnosticReporter = diagnosticReporter;

        // FIXME Job is not made immediately visible after it is accepted by reconciliation framework
        rethrow(() -> Thread.sleep(1000));

        TestStreamObserver<JobChangeNotification> jobEvents = new TestStreamObserver<>();
        ConnectableObservable<JobChangeNotification> connectableEventStream = jobEvents.toObservable()
                .doOnNext(event -> logger.info("Received job change notification: {}", event))
                .replay();

        connectableEventStream.filter(e -> e.getNotificationCase() == NotificationCase.JOBUPDATE)
                .map(n -> toCoreJob(n.getJobUpdate().getJob()))
                .subscribe(jobEventStream);

        connectableEventStream.filter(e -> e.getNotificationCase() == NotificationCase.TASKUPDATE)
                .map(event -> event.getTaskUpdate().getTask())
                .subscribe(
                        grpcTask -> {
                            Task coreTask = GrpcJobManagementModelConverters.toCoreTask(getJob(), grpcTask);
                            String taskId = coreTask.getId();
                            TaskHolder taskHolder = taskHolders.get(taskId);
                            if (taskHolder == null) {
                                Optional<String> resubmitOfOpt = coreTask.getResubmitOf();
                                Integer slot = null;
                                if (resubmitOfOpt.isPresent()) {
                                    String resubmitOf = resubmitOfOpt.get();
                                    slot = taskToSlot.get(resubmitOf);
                                }
                                if (slot == null) {
                                    slot = nextIndex++;
                                }
                                taskSlotIndexes.put(slot, taskId);
                                taskToSlot.put(taskId, slot);
                                taskHolders.put(taskId, taskHolder = new TaskHolder());
                            }
                            taskHolder.onNext(coreTask);
                        },
                        e -> logger.error("Task event stream in job {} terminated with an error", jobId, e),
                        () -> logger.info("Task event stream in job {} completed", jobId)
                );
        Observable<JobChangeNotification> snapshotMarker = connectableEventStream.filter(e -> e.getNotificationCase() == NotificationCase.SNAPSHOTEND);

        this.eventStreamSubscription = connectableEventStream.connect();
        client.observeJob(JobId.newBuilder().setId(jobId).build(), jobEvents);

        snapshotMarker.take(1).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).toBlocking().first();
    }

    void stop() {
        eventStreamSubscription.unsubscribe();
    }

    public JobsScenarioBuilder toJobs() {
        return jobsScenarioBuilder;
    }

    public String getJobId() {
        return jobId;
    }

    public Job getJob() {
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> jobEventStream.getLatestItem() != null);
        return Preconditions.checkNotNull(jobEventStream.getLatestItem(), "Job not created yet");
    }

    public TaskScenarioBuilder getTask(String taskId) {
        return taskHolders.get(taskId).getTaskScenarioBuilder();
    }

    public TaskScenarioBuilder getTaskByIndex(int idx) {
        List<TaskHolder> lastTaskHolders = getLastTaskHolders();
        Preconditions.checkArgument(lastTaskHolders.size() > idx, "Task with index %s not created yet", idx);
        return lastTaskHolders.get(idx).getTaskScenarioBuilder();
    }

    public TaskScenarioBuilder getTaskInSlot(int slot, int resubmit) {
        Collection<String> taskIdsPerSlot = taskSlotIndexes.get(slot);
        Preconditions.checkArgument(resubmit < taskIdsPerSlot.size(), "Task with index %s and resubmit=%s not created yet", slot, resubmit);
        String taskId = taskIdsPerSlot.stream().skip(resubmit).findFirst().get();
        return taskHolders.get(taskId).getTaskScenarioBuilder();
    }

    public JobScenarioBuilder template(Function<JobScenarioBuilder, JobScenarioBuilder> templateFun) {
        return templateFun.apply(this);
    }

    public JobScenarioBuilder allTasks(Function<TaskScenarioBuilder, TaskScenarioBuilder> taskActions) {
        return inTasks(t -> true, taskActions);
    }

    public JobScenarioBuilder inTasks(Predicate<TaskScenarioBuilder> predicate, Function<TaskScenarioBuilder, TaskScenarioBuilder> taskActions) {
        List<TaskHolder> lastTaskHolders = getLastTaskHolders();
        lastTaskHolders.forEach(taskHolder -> {
            if (predicate.test(taskHolder.getTaskScenarioBuilder())) {
                taskActions.apply(taskHolder.getTaskScenarioBuilder());
            }
        });
        return this;
    }

    public JobScenarioBuilder inTask(int idx, Function<TaskScenarioBuilder, TaskScenarioBuilder> taskActions) {
        Preconditions.checkArgument(idx < nextIndex, "No task with id %s in job %s", idx, jobId);
        taskActions.apply(getTaskByIndex(idx));
        return this;
    }

    public JobScenarioBuilder inTask(int idx, int resubmit, Function<TaskScenarioBuilder, TaskScenarioBuilder> taskActions) {
        Preconditions.checkArgument(idx < nextIndex, "No task with id %s in job %s", idx, jobId);
        taskActions.apply(getTaskInSlot(idx, resubmit));
        return this;
    }

    public JobScenarioBuilder updateJobCapacity(Capacity capacity) {
        logger.info("[{}] Changing job {} capacity to {}...", discoverActiveTest(), jobId, capacity);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        client.updateJobCapacity(
                JobCapacityUpdate.newBuilder().setJobId(jobId).setCapacity(toGrpcCapacity(capacity)).build(),
                responseObserver
        );
        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        expectJobUpdateEvent(job -> {
            ServiceJobExt ext = (ServiceJobExt) job.getJobDescriptor().getExtensions();
            return ext.getCapacity().equals(capacity);
        }, "Job capacity update did not complete in time");

        logger.info("[{}] Job {} scaled to new size in {}ms", discoverActiveTest(), jobId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public JobScenarioBuilder updateJobCapacityDesired(int desired, int unchangedMin, int unchangedMax) {
        logger.info("[{}] Changing job {} capacity desired to {}...", discoverActiveTest(), jobId, desired);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();

        client.updateJobCapacityWithOptionalAttributes(
                JobCapacityUpdateWithOptionalAttributes.newBuilder().setJobId(jobId)
                        .setJobCapacityWithOptionalAttributes(JobCapacityWithOptionalAttributes.newBuilder().setDesired(UInt32Value.newBuilder().setValue(desired).build()).build()).build(),
                responseObserver);

        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        expectJobUpdateEvent(job -> {
            ServiceJobExt ext = (ServiceJobExt) job.getJobDescriptor().getExtensions();
            Capacity capacity = ext.getCapacity();
            return capacity.getDesired() == desired && capacity.getMin() == unchangedMin && capacity.getMax() == unchangedMax;
        }, "Job capacity update did not complete in time");

        logger.info("[{}] Job {} scaled to new desired size in {}ms", discoverActiveTest(), jobId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public JobScenarioBuilder updateJobCapacityDesiredInvalid(int targetDesired, int currentDesired) {
        logger.info("[{}] Changing job {} capacity desired to {}...", discoverActiveTest(), jobId, targetDesired);
        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        client.updateJobCapacityWithOptionalAttributes(
                JobCapacityUpdateWithOptionalAttributes.newBuilder().setJobId(jobId)
                        .setJobCapacityWithOptionalAttributes(JobCapacityWithOptionalAttributes.newBuilder().setDesired(UInt32Value.newBuilder().setValue(targetDesired).build()).build()).build(),
                responseObserver);

        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(responseObserver::hasError);
        Throwable error = responseObserver.getError();
        assertThat(error).isNotNull();
        assertThat(error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException statusRuntimeException = (StatusRuntimeException) error;
        assertThat(statusRuntimeException.getStatus().getCode() == Status.Code.INVALID_ARGUMENT).isTrue();

        // Make sure desired count is unchanged
        Job job = getJob();
        JobDescriptor.JobDescriptorExt ext = job.getJobDescriptor().getExtensions();
        int currentCapacity = ext instanceof BatchJobExt ? ((BatchJobExt) ext).getSize() : ((ServiceJobExt) ext).getCapacity().getDesired();
        assertThat(currentCapacity).isEqualTo(currentDesired);
        return this;
    }

    public JobScenarioBuilder updateJobCapacityMaxInvalid(int targetMax) {
        logger.info("[{}] Changing job {} capacity max to {}...", discoverActiveTest(), jobId, targetMax);
        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        client.updateJobCapacityWithOptionalAttributes(
                JobCapacityUpdateWithOptionalAttributes.newBuilder().setJobId(jobId)
                        .setJobCapacityWithOptionalAttributes(JobCapacityWithOptionalAttributes.newBuilder().setMax(UInt32Value.newBuilder().setValue(targetMax).build()).build()).build(),
                responseObserver);

        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(responseObserver::hasError);
        Throwable error = responseObserver.getError();
        assertThat(error).isNotNull();
        assertThat(error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException statusRuntimeException = (StatusRuntimeException) error;
        assertThat(statusRuntimeException.getStatus().getCode() == Status.Code.INVALID_ARGUMENT).isTrue();

        return this;
    }

    public JobScenarioBuilder updateJobCapacityMin(int min, int unchangedMax, int unchangedDesired) {
        logger.info("[{}] Changing job {} capacity min to {}...", discoverActiveTest(), jobId, min);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();

        client.updateJobCapacityWithOptionalAttributes(
                JobCapacityUpdateWithOptionalAttributes.newBuilder().setJobId(jobId)
                        .setJobCapacityWithOptionalAttributes(JobCapacityWithOptionalAttributes.newBuilder().setMin(UInt32Value.newBuilder().setValue(min).build()).build()).build(),
                responseObserver);

        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        expectJobUpdateEvent(job -> {
            ServiceJobExt ext = (ServiceJobExt) job.getJobDescriptor().getExtensions();
            Capacity capacity = ext.getCapacity();
            return capacity.getMin() == min && capacity.getMax() == unchangedMax && capacity.getDesired() == unchangedDesired;
        }, "Job capacity update did not complete in time");

        logger.info("[{}] Job {} scaled to new min size in {}ms", discoverActiveTest(), jobId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public JobScenarioBuilder updateJobCapacityMax(int max, int unchangedMin, int unchangedDesired) {
        logger.info("[{}] Changing job {} capacity max to {}...", discoverActiveTest(), jobId, max);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();

        client.updateJobCapacityWithOptionalAttributes(
                JobCapacityUpdateWithOptionalAttributes.newBuilder().setJobId(jobId)
                        .setJobCapacityWithOptionalAttributes(JobCapacityWithOptionalAttributes.newBuilder().setMax(UInt32Value.newBuilder().setValue(max).build()).build()).build(),
                responseObserver);

        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        expectJobUpdateEvent(job -> {
            ServiceJobExt ext = (ServiceJobExt) job.getJobDescriptor().getExtensions();
            Capacity capacity = ext.getCapacity();
            return capacity.getMax() == max && capacity.getMin() == unchangedMin && capacity.getDesired() == unchangedDesired;
        }, "Job capacity update did not complete in time");

        logger.info("[{}] Job {} scaled to new max size in {}ms", discoverActiveTest(), jobId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }


    public JobScenarioBuilder updateJobStatus(boolean enabled) {
        logger.info("[{}] Changing job {} enable status to {}...", discoverActiveTest(), jobId, enabled);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        client.updateJobStatus(JobStatusUpdate.newBuilder().setId(jobId).setEnableStatus(enabled).build(), responseObserver);
        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        expectJobUpdateEvent(job -> {
            ServiceJobExt ext = (ServiceJobExt) job.getJobDescriptor().getExtensions();
            return ext.isEnabled() == enabled;
        }, "Job status update did not complete in time");

        logger.info("[{}] Changing job {} enable status to {} finished in {}ms", discoverActiveTest(), jobId, enabled, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public JobScenarioBuilder updateJobDisruptionBudget(DisruptionBudget disruptionBudget) {
        logger.info("[{}] Changing job {} disruption budget to {}...", discoverActiveTest(), jobId, disruptionBudget);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        client.updateJobDisruptionBudget(JobDisruptionBudgetUpdate.newBuilder()
                        .setJobId(jobId)
                        .setDisruptionBudget(toGrpcDisruptionBudget(disruptionBudget))
                        .build(),
                responseObserver
        );
        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        expectJobUpdateEvent(job -> job.getJobDescriptor().getDisruptionBudget().equals(disruptionBudget), "Job disruption budget update did not complete in time");

        logger.info("[{}] Changing job {} disruption budget to {} finished in {}ms", discoverActiveTest(), jobId, disruptionBudget, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public JobScenarioBuilder updateJobAttributes(Map<String, String> attributes) {
        logger.info("[{}] Updating job {} attributes with {}", discoverActiveTest(), jobId, attributes);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        client.updateJobAttributes(JobAttributesUpdate.newBuilder()
                        .setJobId(jobId)
                        .putAllAttributes(attributes)
                        .build(),
                responseObserver
        );
        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        expectJobUpdateEvent(job -> {
            Map<String, String> updatedAttributes = job.getJobDescriptor().getAttributes();
            return updatedAttributes.entrySet().containsAll(attributes.entrySet());
        }, "Job attributes update did not complete in time");

        logger.info("[{}] Changing job {} attributes with {} finished in {}ms", discoverActiveTest(), jobId, attributes, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public JobScenarioBuilder deleteJobAttributes(List<String> keys) {
        logger.info("[{}] Deleting job {} attributes with keys: {}", discoverActiveTest(), jobId, keys);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        client.deleteJobAttributes(JobAttributesDeleteRequest.newBuilder()
                        .setJobId(jobId)
                        .addAllKeys(keys)
                        .build(),
                responseObserver
        );
        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        expectJobUpdateEvent(job -> {
            Map<String, String> updatedAttributes = job.getJobDescriptor().getAttributes();
            return !updatedAttributes.keySet().containsAll(keys);
        }, "Job attributes update did not complete in time");

        logger.info("[{}] Changing job {} attributes with keys: {} finished in {}ms", discoverActiveTest(), jobId, keys, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public JobScenarioBuilder killJob() {
        logger.info("[{}] Killing job {}...", discoverActiveTest(), jobId);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        client.killJob(JobId.newBuilder().setId(jobId).build(), responseObserver);
        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        logger.info("[{}] Job {} killed in {}ms", discoverActiveTest(), jobId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public JobScenarioBuilder expectJobUpdateEvent(Function<Job, Boolean> condition, String message) {
        logger.info("[{}] Expecting job update event with a predicate...", discoverActiveTest());

        Stopwatch stopWatch = Stopwatch.createStarted();
        Job newJob = rethrow(() -> jobEventStream.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        Preconditions.checkState(condition.apply(newJob), "Received Job does not match predicate. %s", message);

        logger.info("[{}] Expected job update event with a matching predicate received in {}ms", discoverActiveTest(), stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public JobScenarioBuilder expectJobEventStreamCompletes() {
        Stopwatch stopWatch = Stopwatch.createStarted();
        try {
            logger.info("[{}] Expect job event stream to complete due to job termination...", discoverActiveTest());

            await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(eventStreamSubscription::isUnsubscribed);

            logger.info("[{}] Job event stream completed after waiting for {}ms", discoverActiveTest(), stopWatch.elapsed(TimeUnit.MILLISECONDS));
            return this;
        } catch (Throwable e) {
            logger.info("[{}] Job event stream failed after waiting for {}ms", discoverActiveTest(), stopWatch.elapsed(TimeUnit.MILLISECONDS), e);
            throw e;
        }
    }

    public JobScenarioBuilder expectAllTasksCreated() {
        JobDescriptor.JobDescriptorExt ext = getJob().getJobDescriptor().getExtensions();
        int size = ext instanceof BatchJobExt ? ((BatchJobExt) ext).getSize() : ((ServiceJobExt) ext).getCapacity().getDesired();

        logger.info("[{}] Expecting {} tasks to be active...", discoverActiveTest(), size);
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> getNonFinishedTaskCount() == size);
        return this;
    }

    public JobScenarioBuilder expectTasksOnAgents(int count) {
        logger.info("[{}] Expecting {} tasks to be running on agents...", discoverActiveTest(), count);
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> getLastTaskHolders().stream()
                .filter(h -> h.getTaskScenarioBuilder().hasTaskExecutorHolder())
                .count() == count);
        return this;
    }

    public JobScenarioBuilder expectJobToScaleDown() {
        JobDescriptor.JobDescriptorExt ext = getJob().getJobDescriptor().getExtensions();
        Preconditions.checkState(ext instanceof ServiceJobExt, "Not a service job %s", jobId);

        int size = ((ServiceJobExt) ext).getCapacity().getDesired();

        logger.info("[{}] Expect job {} to scale down to the desired size {}...", discoverActiveTest(), jobId, size);
        Stopwatch stopWatch = Stopwatch.createStarted();

        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> {
            List<TaskHolder> lastTaskHolders = getLastTaskHolders();
            return lastTaskHolders.stream().filter(t -> {
                TaskState state = t.getTaskScenarioBuilder().getTask().getStatus().getState();
                return state != TaskState.Finished;

            }).count() <= size;
        });

        logger.info("[{}] Expected job {} scale down to the desired size {} completed in {}ms", discoverActiveTest(), jobId, size, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public JobScenarioBuilder expectTaskInSlot(int slot, int index) {
        logger.info("[{}] Expecting task in slot {} with index {} to exist", discoverActiveTest(), slot, index);
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> {
            Collection<String> taskIdsPerSlot = taskSlotIndexes.get(slot);
            return index < taskIdsPerSlot.size() && Iterables.get(taskIdsPerSlot, index) != null;
        });
        return this;
    }

    public JobScenarioBuilder expectSome(int count, Predicate<TaskScenarioBuilder> predicate) {
        logger.info("[{}] Expecting {} tasks to fulfill the predicate requirements", discoverActiveTest(), count);
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> {
            long matching = getLastTaskHolders().stream().filter(t -> predicate.test(t.getTaskScenarioBuilder())).count();
            return matching == count;
        });
        return this;
    }

    public JobScenarioBuilder inJob(Consumer<Job<?>> consumer) {
        consumer.accept(getJob());
        return this;
    }

    public JobScenarioBuilder assertJob(Predicate<Job> jobPredicate) {
        if (!jobPredicate.test(getJob())) {
            throw new IllegalStateException("Job predicate is false");
        }
        return this;
    }

    public JobScenarioBuilder assertTasks(Predicate<List<Task>> tasksPredicate) {
        List<TaskHolder> lastTaskHolders = getLastTaskHolders();
        List<Task> tasks = lastTaskHolders.stream().map(h -> h.getTaskScenarioBuilder().getTask()).collect(Collectors.toList());
        if (!tasksPredicate.test(tasks)) {
            throw new IllegalStateException("Tasks predicate is false");
        }
        return this;
    }

    public JobScenarioBuilder assertEachTask(Predicate<Task> taskPredicate, String message) {
        List<TaskHolder> lastTaskHolders = getLastTaskHolders();
        boolean allMatch = lastTaskHolders.stream().allMatch(h -> taskPredicate.test(h.getTaskScenarioBuilder().getTask()));
        if (!allMatch) {
            throw new IllegalStateException("Task predicate is false for one or more tasks. " + message);
        }
        return this;
    }

    public JobScenarioBuilder assertEachContainer(Predicate<TaskExecutorHolder> taskExecutorHolderPredicate, String message) {
        List<TaskHolder> lastTaskHolders = getLastTaskHolders();
        boolean allMatch = lastTaskHolders.stream().allMatch(task ->
                taskExecutorHolderPredicate.test(task.getTaskScenarioBuilder().getTaskExecutionHolder())
        );
        if (!allMatch) {
            throw new IllegalStateException("TaskExecutorHolder predicate is false for one or more tasks. " + message);
        }
        return this;
    }

    public int getNonFinishedTaskCount() {
        List<TaskHolder> lastTaskHolders = getLastTaskHolders();
        return (int) lastTaskHolders.stream()
                .filter(h -> h.getTaskScenarioBuilder().getTask().getStatus().getState() != TaskState.Finished)
                .count();
    }

    public JobScenarioBuilder findTasks(TaskQuery taskQuery, Predicate<List<com.netflix.titus.grpc.protogen.Task>> tasksPredicate) {
        TestStreamObserver<TaskQueryResult> responseObserver = new TestStreamObserver<>();
        client.findTasks(taskQuery, responseObserver);
        TaskQueryResult result = rethrow(() -> responseObserver.getLast(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        if (result == null) {
            throw new IllegalStateException("TaskQueryResult is null");
        } else if (!tasksPredicate.test(result.getItemsList())) {
            throw new IllegalStateException("Tasks predicate is false");
        }
        return this;
    }

    public JobScenarioBuilder andThen(Runnable action) {
        action.run();
        return this;
    }

    private class TaskHolder {
        private final ReplaySubject<Task> taskEventStream;
        private final TaskScenarioBuilder taskScenarioBuilder;

        private TaskHolder() {
            this.taskEventStream = ReplaySubject.create();
            this.taskScenarioBuilder = new TaskScenarioBuilder(titusOperations, JobScenarioBuilder.this, taskEventStream,
                    schedulingService, diagnosticReporter);
        }

        private TaskScenarioBuilder getTaskScenarioBuilder() {
            return taskScenarioBuilder;
        }

        private void onNext(Task task) {
            taskEventStream.onNext(task);
        }
    }

    private List<TaskHolder> getLastTaskHolders() {
        return getLastElementPerKey(taskSlotIndexes).stream().map(taskHolders::get).collect(Collectors.toList());
    }

    private List<String> getLastElementPerKey(Multimap<Integer, String> multimap) {
        List<String> elements = new ArrayList<>();
        for (Collection<String> collection : multimap.asMap().values()) {
            if (!collection.isEmpty()) {
                String last = Iterables.getLast(collection);
                elements.add(last);
            }
        }
        return elements;
    }
}
