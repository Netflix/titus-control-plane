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

package com.netflix.titus.supplementary.relocation.workflow.step;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.RelocationMetrics;
import com.netflix.titus.supplementary.relocation.util.RelocationPredicates;
import com.netflix.titus.supplementary.relocation.util.RelocationPredicates.RelocationTrigger;
import com.netflix.titus.supplementary.relocation.util.RelocationUtil;

/**
 * Reports both the legacy and the current relocation needs.
 */
public class RelocationMetricsStep {

    private static final String JOB_REMAINING_RELOCATION_METRICS = RelocationMetrics.METRIC_ROOT + "jobs";
    private static final String TASK_REMAINING_RELOCATION_METRICS = RelocationMetrics.METRIC_ROOT + "tasks";

    private final ReadOnlyAgentOperations agentOperations;
    private final ReadOnlyJobOperations jobOperations;
    private final Registry registry;

    private final Map<String, JobMetrics> metrics = new HashMap<>();

    public RelocationMetricsStep(ReadOnlyAgentOperations agentOperations,
                                 ReadOnlyJobOperations jobOperations,
                                 TitusRuntime titusRuntime) {
        this.agentOperations = agentOperations;
        this.jobOperations = jobOperations;
        this.registry = titusRuntime.getRegistry();
    }

    public void updateMetrics() {
        Map<String, AgentInstance> taskToInstanceMap = RelocationUtil.buildTasksToInstanceMap(agentOperations, jobOperations);

        Set<String> jobIds = new HashSet<>();
        jobOperations.getJobsAndTasks().forEach(jobAndTask -> {
            Job<?> job = jobAndTask.getLeft();
            jobIds.add(job.getId());
            metrics.computeIfAbsent(job.getId(), jid -> new JobMetrics(job)).update(job, jobAndTask.getRight(), taskToInstanceMap);
        });

        // Remove jobs no longer running.
        Set<String> toRemove = new HashSet<>();
        metrics.keySet().forEach(jobId -> {
            if (!jobIds.contains(jobId)) {
                metrics.get(jobId).remove();
                toRemove.add(jobId);
            }
        });
        toRemove.forEach(metrics::remove);
    }

    private class JobMetrics {

        private final boolean legacy;
        private final String legacyPolicyName;

        private Job<?> job;
        private List<Task> tasks;

        private final Id jobsRemainingId;
        private final Id tasksRemainingId;

        JobMetrics(Job<?> job) {
            this.job = job;
            this.legacy = DisruptionBudgetFunctions.isLegacyJob(job);
            this.legacyPolicyName = legacy ? getLegacyPolicy(job) : null;

            List<Tag> tags = Arrays.asList(
                    new BasicTag("jobId", job.getId()),
                    new BasicTag("application", job.getJobDescriptor().getApplicationName()),
                    new BasicTag("capacityGroup", job.getJobDescriptor().getCapacityGroup()),
                    new BasicTag("legacy", Boolean.toString(legacy))
            );
            this.jobsRemainingId = registry.createId(JOB_REMAINING_RELOCATION_METRICS, tags);
            this.tasksRemainingId = registry.createId(TASK_REMAINING_RELOCATION_METRICS, tags);
        }

        Job<?> getJob() {
            return job;
        }

        void update(Job<?> latestJob, List<Task> latestTasks, Map<String, AgentInstance> taskToInstanceMap) {
            this.job = latestJob;
            this.tasks = latestTasks;

            if (legacy) {
                updateLegacy(taskToInstanceMap);
            } else {
                updateJobWithDisruptionBudget(taskToInstanceMap);
            }
        }

        private void updateLegacy(Map<String, AgentInstance> taskToInstanceMap) {
            int noRelocation = 0;
            int evacuatedAgentMatches = 0;

            for (Task task : tasks) {
                AgentInstance instance = taskToInstanceMap.get(task.getId());
                AgentInstanceGroup instanceGroup = getInstanceGroupOf(instance);
                if (instance == null || instanceGroup == null) {
                    noRelocation++;
                } else {
                    if (RelocationPredicates.isRelocationRequired(instance) || RelocationPredicates.isRelocationRequired(instanceGroup)) {
                        evacuatedAgentMatches++;
                    } else {
                        noRelocation++;
                    }
                }
            }

            update(noRelocation, evacuatedAgentMatches, 0, 0, 0);
        }

        private void updateJobWithDisruptionBudget(Map<String, AgentInstance> taskToInstanceMap) {
            if (tasks.isEmpty()) {
                remove();
            } else {
                updateTasks(taskToInstanceMap);
            }
        }

        private void updateTasks(Map<String, AgentInstance> taskToInstanceMap) {
            int noRelocation = 0;
            int evacuatedAgentMatches = 0;
            int jobRelocationRequestMatches = 0;
            int taskRelocationRequestMatches = 0;
            int taskRelocationUnrecognized = 0;

            for (Task task : tasks) {
                AgentInstance instance = taskToInstanceMap.get(task.getId());
                AgentInstanceGroup instanceGroup = getInstanceGroupOf(instance);
                if (instance == null || instanceGroup == null) {
                    noRelocation++;
                } else {
                    RelocationTrigger trigger = Evaluators
                            .firstPresent(
                                    () -> RelocationPredicates.isRelocationRequired(instanceGroup) ? Optional.of(RelocationTrigger.InstanceGroup) : Optional.empty(),
                                    () -> RelocationPredicates.checkIfMustBeRelocatedImmediately(job, task, instance).map(Pair::getLeft),
                                    () -> RelocationPredicates.checkIfRelocationRequired(job, task, instance).map(Pair::getLeft)
                            )
                            .orElse(null);

                    if (trigger != null) {
                        switch (trigger) {
                            case Instance:
                                evacuatedAgentMatches++;
                                break;
                            case Job:
                                jobRelocationRequestMatches++;
                                break;
                            case Task:
                                taskRelocationRequestMatches++;
                                break;
                            default:
                                taskRelocationUnrecognized++;
                        }
                    } else {
                        noRelocation++;
                    }
                }
            }

            update(noRelocation, evacuatedAgentMatches, jobRelocationRequestMatches, taskRelocationRequestMatches, taskRelocationUnrecognized);
        }

        private void update(int noRelocation, int evacuatedAgentMatches, int jobRelocationRequestMatches, int taskRelocationRequestMatches, int taskRelocationUnrecognized) {
            String policyType;
            if (legacy) {
                policyType = legacyPolicyName;
            } else {
                policyType = job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy().getClass().getSimpleName();
            }

            // Job level
            int totalToRelocate = evacuatedAgentMatches + jobRelocationRequestMatches + taskRelocationRequestMatches + taskRelocationUnrecognized;
            registry.gauge(jobsRemainingId.withTags(
                    "relocationRequired", "false",
                    "policy", policyType
            )).set((totalToRelocate == 0 && noRelocation > 0) ? 1 : 0);
            registry.gauge(jobsRemainingId.withTags(
                    "relocationRequired", "true",
                    "policy", policyType
            )).set(totalToRelocate > 0 ? 1 : 0);

            // Task aggregates
            registry.gauge(tasksRemainingId.withTags(
                    "trigger", "noRelocation",
                    "policy", policyType
            )).set(noRelocation);

            registry.gauge(tasksRemainingId.withTags(
                    "trigger", "evacuatedAgents",
                    "policy", policyType
            )).set(evacuatedAgentMatches);

            registry.gauge(tasksRemainingId.withTags(
                    "trigger", "jobRelocationRequest",
                    "policy", policyType
            )).set(jobRelocationRequestMatches);

            registry.gauge(tasksRemainingId.withTags(
                    "trigger", "taskRelocationRequest",
                    "policy", policyType
            )).set(taskRelocationRequestMatches);

            registry.gauge(tasksRemainingId.withTags(
                    "trigger", "unrecognized",
                    "policy", policyType
            )).set(taskRelocationUnrecognized);

            registry.gauge(tasksRemainingId.withTags(
                    "trigger", "unrecognized",
                    "policy", policyType
            )).set(taskRelocationUnrecognized);
        }

        void remove() {
            update(0, 0, 0, 0, 0);
        }

        private String getLegacyPolicy(Job<?> job) {
            if (JobFunctions.isServiceJob(job)) {
                return ((ServiceJobExt) job.getJobDescriptor().getExtensions()).getMigrationPolicy().getClass().getSimpleName();
            }
            return "legacyBatch";
        }

        private AgentInstanceGroup getInstanceGroupOf(AgentInstance instance) {
            return instance == null ? null : agentOperations.findInstanceGroup(instance.getInstanceGroupId()).orElse(null);
        }
    }
}
