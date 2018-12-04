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

package com.netflix.titus.master.eviction.service;

import java.util.List;
import java.util.Optional;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.model.event.TaskTerminationEvent;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.eviction.service.EvictionException.ErrorCode;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.eviction.service.quota.ConsumptionResult;
import com.netflix.titus.master.eviction.service.quota.TitusQuotasManager;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import com.netflix.titus.testkit.model.job.JobGenerator;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TaskTerminationExecutorTest {

    private static final String EVICTION_REASON = "Test eviction";

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final JobComponentStub jobComponentStub = new JobComponentStub(titusRuntime);

    private final TitusQuotasManager quotasManager = Mockito.mock(TitusQuotasManager.class);

    private final TitusRxSubscriber<EvictionEvent> eventSubscriber = new TitusRxSubscriber<>();

    @Test
    public void testTerminateTaskWithEnoughQuota() {
        Pair<Job, List<Task>> jobAndTask = createAndStartJobWithTasks(1);
        Job<?> job = jobAndTask.getLeft();
        Task task = jobAndTask.getRight().get(0);

        when(quotasManager.findEvictionQuota(Reference.job(job.getId()))).thenReturn(Optional.of(EvictionQuota.jobQuota(job.getId(), 1, "Test")));
        when(quotasManager.tryConsumeQuota(job, task)).thenReturn(ConsumptionResult.approved());

        StepVerifier.withVirtualTime(
                () -> newTerminationExecutor().terminateTask(task.getId(), EVICTION_REASON, "callerContext")
        ).verifyComplete();

        expectTaskTerminationEvent(task, true);
        verify(quotasManager, times(1)).tryConsumeQuota(job, task);
    }

    @Test
    public void testTerminateTaskWithNoQuota() {
        Pair<Job, List<Task>> jobAndTask = createAndStartJobWithTasks(1);
        Job<?> job = jobAndTask.getLeft();
        Task task = jobAndTask.getRight().get(0);

        when(quotasManager.tryConsumeQuota(job, task)).thenReturn(ConsumptionResult.rejected("no quota"));

        StepVerifier
                .withVirtualTime(() -> newTerminationExecutor().terminateTask(task.getId(), EVICTION_REASON, "callerContext"))
                .consumeErrorWith(e -> expectEvictionError(e, ErrorCode.NoQuota))
                .verify();

        expectTaskTerminationEvent(task, false);
        verify(quotasManager, times(1)).tryConsumeQuota(job, task);
    }

    @Test
    public void testTerminateTwoTasksWithQuotaForOne() {
        Pair<Job, List<Task>> jobAndTasks = createAndStartJobWithTasks(2);
        Job<?> job = jobAndTasks.getLeft();
        List<Task> tasks = jobAndTasks.getRight();

        when(quotasManager.tryConsumeQuota(job, tasks.get(0))).thenReturn(ConsumptionResult.approved());
        when(quotasManager.tryConsumeQuota(job, tasks.get(1))).thenReturn(ConsumptionResult.rejected("no quota"));

        StepVerifier
                .withVirtualTime(() -> {
                    TaskTerminationExecutor executor = newTerminationExecutor();
                    return terminate(executor, tasks.get(0)).mergeWith(terminate(executor, tasks.get(1))).collectList();
                })
                .assertNext(next -> {
                    long succeededCount = next.stream().filter(p -> !p.isPresent()).count();
                    Optional<Throwable> failed = next.stream().filter(Optional::isPresent).map(Optional::get).findFirst();

                    assertThat(succeededCount).isEqualTo(1);
                    assertThat(failed).isPresent();
                    expectEvictionError(failed.get(), ErrorCode.NoQuota);
                })
                .verifyComplete();

    }

    private Flux<Optional<Throwable>> terminate(TaskTerminationExecutor executor, Task task) {
        return executor.terminateTask(task.getId(), EVICTION_REASON, "callerContext")
                .materialize()
                .map(signal -> (Optional<Throwable>) (signal.isOnError() ? Optional.of(signal.getThrowable()) : Optional.empty()))
                .flux();
    }

    private TaskTerminationExecutor newTerminationExecutor() {
        TaskTerminationExecutor executor = new TaskTerminationExecutor(
                jobComponentStub.getJobOperations(),
                quotasManager,
                titusRuntime,
                Schedulers.parallel()
        );
        executor.events().subscribe(eventSubscriber);
        return executor;
    }

    private Pair<Job, List<Task>> createAndStartJobWithTasks(int taskCount) {
        Job job = jobComponentStub.createJob(JobGenerator.batchJobs(oneTaskBatchJobDescriptor().but(JobFunctions.ofBatchSize(taskCount))).getValue());
        List<Task> tasks = jobComponentStub.createDesiredTasks(job);
        tasks.forEach(t -> jobComponentStub.moveTaskToState(t, TaskState.Started));
        return Pair.of(job, jobComponentStub.getJobOperations().getTasks(job.getId()));
    }

    private void expectEvictionError(Throwable error, ErrorCode expectedErrorCode) {
        assertThat(error).isInstanceOf(EvictionException.class);
        assertThat(((EvictionException) error).getErrorCode()).isEqualTo(expectedErrorCode);
    }

    private void expectTaskTerminationEvent(Task task, boolean approved) {
        EvictionEvent event = eventSubscriber.takeNext();
        assertThat(event).isInstanceOf(TaskTerminationEvent.class);

        TaskTerminationEvent terminationEvent = (TaskTerminationEvent) event;
        assertThat(terminationEvent.getTaskId()).isEqualTo(task.getId());
        assertThat(terminationEvent.isApproved()).isEqualTo(approved);
        if (!approved) {
            assertThat(terminationEvent.getError().get().getMessage()).contains("no quota");
        }
    }
}