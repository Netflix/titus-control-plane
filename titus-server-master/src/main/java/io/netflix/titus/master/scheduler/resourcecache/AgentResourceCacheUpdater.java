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

package io.netflix.titus.master.scheduler.resourcecache;

import java.util.Optional;
import java.util.function.Function;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.model.event.TaskStateChangeEvent;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST;

public class AgentResourceCacheUpdater {
    private static final Logger logger = LoggerFactory.getLogger(AgentResourceCacheUpdater.class);

    private final TitusRuntime titusRuntime;
    private final AgentResourceCache agentResourceCache;
    private final V3JobOperations v3JobOperations;
    private final RxEventBus rxEventBus;

    private Subscription v2TaskSubscription;
    private Subscription v3TaskSubscription;

    public AgentResourceCacheUpdater(TitusRuntime titusRuntime,
                                     AgentResourceCache agentResourceCache,
                                     V3JobOperations v3JobOperations,
                                     RxEventBus rxEventBus) {
        this.titusRuntime = titusRuntime;
        this.agentResourceCache = agentResourceCache;
        this.v3JobOperations = v3JobOperations;
        this.rxEventBus = rxEventBus;
    }

    public void start() {
        Observable<TaskStateChangeEvent> v2TaskStream = rxEventBus.listen(getClass().getSimpleName(), TaskStateChangeEvent.class)
                .filter(taskStateChangeEvent -> taskStateChangeEvent.getSource() instanceof Pair);
        v2TaskSubscription = titusRuntime.persistentStream(v2TaskStream).subscribe(
                this::createOrUpdateAgentResourceCacheForV2Task,
                e -> logger.error("Unable to update agent resource cache for v2 task with error: ", e),
                () -> logger.info("Finished updating agent resource cache for v2 tasks")
        );

        Observable<TaskUpdateEvent> v3TaskStream = v3JobOperations.observeJobs()
                .filter(event -> event instanceof TaskUpdateEvent)
                .cast(TaskUpdateEvent.class);
        v3TaskSubscription = titusRuntime.persistentStream(v3TaskStream).subscribe(
                this::createOrUpdateAgentResourceCacheForV3Task,
                e -> logger.error("Unable to update agent resource cache for v3 task with error: ", e),
                () -> logger.info("Finished updating agent resource cache for v3 tasks")
        );
    }

    public void shutdown() {
        if (!v2TaskSubscription.isUnsubscribed()) {
            v2TaskSubscription.unsubscribe();
        }
        if (!v3TaskSubscription.isUnsubscribed()) {
            v3TaskSubscription.unsubscribe();
        }
    }

    private void createOrUpdateAgentResourceCacheForV2Task(TaskStateChangeEvent event) {
        @SuppressWarnings("unchecked")
        Pair<V2JobMetadata, V2WorkerMetadata> jobAndTaskPair = (Pair<V2JobMetadata, V2WorkerMetadata>) event.getSource();
        V2JobMetadata job = jobAndTaskPair.getLeft();
        V2WorkerMetadata task = jobAndTaskPair.getRight();
        String hostname = task.getSlave();
        if (task.getState() == V2JobState.Started) {
            agentResourceCache.createOrUpdateActive(hostname, instanceOpt -> {
                AgentResourceCacheInstance instance = AgentResourceCacheFunctions.createInstance(hostname, job, task, titusRuntime.getClock().wallTime());
                if (instanceOpt.isPresent()) {
                    return AgentResourceCacheFunctions.mergeInstances(instanceOpt.get(), instance);
                }
                return instance;
            });
        } else if (V2JobState.isTerminalState(task.getState())) {
            Function<Optional<AgentResourceCacheInstance>, AgentResourceCacheInstance> finishedFunction = instanceOpt -> {
                if (instanceOpt.isPresent()) {
                    AgentResourceCacheInstance existingInstance = instanceOpt.get();
                    return AgentResourceCacheFunctions.removeTaskFromInstance(existingInstance, task, titusRuntime.getClock().wallTime());
                }
                return null;
            };
            agentResourceCache.createOrUpdateIdle(hostname, finishedFunction);
            agentResourceCache.createOrUpdateActive(hostname, finishedFunction);
        }
    }

    private void createOrUpdateAgentResourceCacheForV3Task(TaskUpdateEvent event) {
        Job<?> job = event.getCurrentJob();
        Task task = event.getCurrentTask();
        String hostname = task.getTaskContext().get(TASK_ATTRIBUTES_AGENT_HOST);
        if (task.getStatus().getState() == TaskState.Started) {
            agentResourceCache.createOrUpdateActive(hostname, instanceOpt -> {
                AgentResourceCacheInstance instance = AgentResourceCacheFunctions.createInstance(hostname, job, task, titusRuntime.getClock().wallTime());
                if (instanceOpt.isPresent()) {
                    return AgentResourceCacheFunctions.mergeInstances(instanceOpt.get(), instance);
                }
                return instance;
            });
        } else if (task.getStatus().getState() == TaskState.Finished) {
            Function<Optional<AgentResourceCacheInstance>, AgentResourceCacheInstance> finishedFunction = instanceOpt -> {
                if (instanceOpt.isPresent()) {
                    AgentResourceCacheInstance existingInstance = instanceOpt.get();
                    return AgentResourceCacheFunctions.removeTaskFromInstance(existingInstance, task, titusRuntime.getClock().wallTime());
                }
                return null;
            };
            agentResourceCache.createOrUpdateIdle(hostname, finishedFunction);
            agentResourceCache.createOrUpdateActive(hostname, finishedFunction);
        }
    }
}
