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

package com.netflix.titus.master.scheduler.resourcecache;

import com.netflix.fenzo.PreferentialNamedConsumableResourceSet.ConsumeResult;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID;

public class AgentResourceCacheUpdater {
    private static final Logger logger = LoggerFactory.getLogger(AgentResourceCacheUpdater.class);

    private final TitusRuntime titusRuntime;
    private final AgentResourceCache agentResourceCache;
    private final V3JobOperations v3JobOperations;

    private Subscription v3TaskSubscription;

    public AgentResourceCacheUpdater(TitusRuntime titusRuntime,
                                     AgentResourceCache agentResourceCache,
                                     V3JobOperations v3JobOperations) {
        this.titusRuntime = titusRuntime;
        this.agentResourceCache = agentResourceCache;
        this.v3JobOperations = v3JobOperations;
    }

    public void start() {
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
        ObservableExt.safeUnsubscribe(v3TaskSubscription);
    }

    public void createOrUpdateAgentResourceCacheForTask(QueuableTask task, String hostname) {
        ConsumeResult consumeResult = CollectionsExt.first(task.getAssignedResources().getConsumedNamedResources());
        long time = titusRuntime.getClock().wallTime();
        AgentResourceCacheInstance instance = AgentResourceCacheFunctions.createInstance(hostname, task, consumeResult, time);
        agentResourceCache.createOrUpdate(hostname, instanceOpt -> {
            if (instanceOpt.isPresent()) {
                return AgentResourceCacheFunctions.updateInstance(instanceOpt.get(), instance);
            }
            return instance;
        });
    }

    private void createOrUpdateAgentResourceCacheForV3Task(TaskUpdateEvent event) {
        Job<?> job = event.getCurrentJob();
        Task task = event.getCurrentTask();
        String hostname = task.getTaskContext().get(TASK_ATTRIBUTES_AGENT_INSTANCE_ID);
        if (task.getStatus().getState() == TaskState.Started) {
            agentResourceCache.createOrUpdate(hostname, instanceOpt -> {
                AgentResourceCacheInstance instance = AgentResourceCacheFunctions.createInstance(hostname, job, task, titusRuntime.getClock().wallTime());
                if (instanceOpt.isPresent()) {
                    return AgentResourceCacheFunctions.updateInstance(instanceOpt.get(), instance);
                }
                return instance;
            });
        } else if (task.getStatus().getState() == TaskState.Finished) {
            agentResourceCache.createOrUpdate(hostname, instanceOpt -> {
                if (instanceOpt.isPresent()) {
                    AgentResourceCacheInstance existingInstance = instanceOpt.get();
                    return AgentResourceCacheFunctions.removeTaskFromInstance(existingInstance, task, titusRuntime.getClock().wallTime());
                }
                return null;
            });
        }
    }
}
