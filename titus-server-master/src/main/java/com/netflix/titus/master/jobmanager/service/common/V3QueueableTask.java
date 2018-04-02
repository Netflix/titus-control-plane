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

package com.netflix.titus.master.jobmanager.service.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.model.job.TitusQueuableTask;
import com.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import com.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import com.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;

import static com.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;

/**
 */
public class V3QueueableTask implements TitusQueuableTask<Job, Task> {

    private static final String DEFAULT_GRP_NAME = "defaultGrp";
    private static final String SecurityGroupsResName = "ENIs";

    private final Job job;
    private final Task task;

    private final double cpus;
    private final double memoryMb;
    private final double networkMbps;
    private final double diskMb;
    private final Map<String, Double> scalarResources;

    private final V3QAttributes qAttributes;

    private List<ConstraintEvaluator> hardConstraints;
    private List<VMTaskFitnessCalculator> softConstraints;
    private final Map<String, NamedResourceSetRequest> namedResources = new HashMap<>();

    private volatile AssignedResources assignedResources;

    public V3QueueableTask(Tier tier,
                           String capacityGroup,
                           Job job,
                           Task task,
                           Supplier<Set<String>> activeTasksGetter,
                           ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
                           SystemSoftConstraint systemSoftConstraint,
                           SystemHardConstraint systemHardConstraint) {
        this.job = job;
        this.task = task;

        ContainerResources containerResources = job.getJobDescriptor().getContainer().getContainerResources();
        this.cpus = containerResources.getCpu();
        this.memoryMb = containerResources.getMemoryMB();
        this.networkMbps = containerResources.getNetworkMbps();
        this.diskMb = containerResources.getDiskMB();
        this.scalarResources = buildScalarResources(job);

        this.qAttributes = new V3QAttributes(tier.ordinal(), capacityGroup);

        List<TwoLevelResource> twoLevelResources = task.getTwoLevelResources();
        if (!isNullOrEmpty(twoLevelResources)) {
            assignedResources = new AssignedResources();
            List<PreferentialNamedConsumableResourceSet.ConsumeResult> consumeResults = new ArrayList<>();
            for (TwoLevelResource resource : twoLevelResources) {
                consumeResults.add(
                        new PreferentialNamedConsumableResourceSet.ConsumeResult(
                                resource.getIndex(), resource.getName(),
                                resource.getValue(), 1.0
                        ));
            }
            assignedResources.setConsumedNamedResources(consumeResults);
        }

        this.softConstraints = toFenzoSoftConstraints(job, systemSoftConstraint, constraintEvaluatorTransformer, activeTasksGetter);
        this.hardConstraints = toFenzoHardConstraints(job, systemHardConstraint, constraintEvaluatorTransformer, activeTasksGetter);

        setupCustomNamedResources(job);
    }

    @Override
    public Job getJob() {
        return job;
    }

    @Override
    public Task getTask() {
        return task;
    }

    @Override
    public String getId() {
        return task.getId();
    }

    @Override
    public String taskGroupName() {
        return DEFAULT_GRP_NAME;
    }

    @Override
    public QAttributes getQAttributes() {
        return qAttributes;
    }

    @Override
    public double getCPUs() {
        return cpus;
    }

    @Override
    public double getMemory() {
        return memoryMb;
    }

    @Override
    public double getNetworkMbps() {
        return networkMbps;
    }

    @Override
    public double getDisk() {
        return diskMb;
    }

    /**
     * Ports are no longer supported.
     */
    @Override
    public int getPorts() {
        return 0;
    }

    @Override
    public Map<String, Double> getScalarRequests() {
        return scalarResources;
    }

    @Override
    public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
        return namedResources;
    }

    @Override
    public List<? extends ConstraintEvaluator> getHardConstraints() {
        return hardConstraints;
    }

    @Override
    public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
        return softConstraints;
    }

    @Override
    public void setAssignedResources(AssignedResources assignedResources) {
        this.assignedResources = assignedResources;
    }

    @Override
    public AssignedResources getAssignedResources() {
        return assignedResources;
    }

    private Map<String, Double> buildScalarResources(Job job) {
        return Collections.singletonMap(Container.RESOURCE_GPU, (double) job.getJobDescriptor().getContainer().getContainerResources().getGpu());
    }

    private List<VMTaskFitnessCalculator> toFenzoSoftConstraints(Job<?> job,
                                                                 SystemSoftConstraint systemSoftConstraint,
                                                                 ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
                                                                 Supplier<Set<String>> activeTasksGetter) {
        List<VMTaskFitnessCalculator> result = new ArrayList<>();
        result.add(systemSoftConstraint);
        job.getJobDescriptor().getContainer().getSoftConstraints().forEach((key, value) ->
                constraintEvaluatorTransformer.softConstraint(Pair.of(key, value), activeTasksGetter).ifPresent(result::add)
        );
        return result;
    }

    private List<ConstraintEvaluator> toFenzoHardConstraints(Job<?> job,
                                                             SystemHardConstraint systemHardConstraint,
                                                             ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer,
                                                             Supplier<Set<String>> runningTasksGetter) {
        List<ConstraintEvaluator> result = new ArrayList<>();
        result.add(systemHardConstraint);
        job.getJobDescriptor().getContainer().getHardConstraints().forEach((key, value) ->
                constraintEvaluatorTransformer.hardConstraint(Pair.of(key, value), runningTasksGetter).ifPresent(result::add)
        );
        return result;
    }

    private void setupCustomNamedResources(Job<?> job) {
        Container container = job.getJobDescriptor().getContainer();
        List<String> securityGroups = container.getSecurityProfile().getSecurityGroups();

        if (!CollectionsExt.isNullOrEmpty(securityGroups)) {
            NamedResourceSetRequest resourceSetRequest = new NamedResourceSetRequest(
                    SecurityGroupsResName,
                    getConcatenatedSecurityGroups(securityGroups),
                    1,
                    1
            );
            namedResources.put(resourceSetRequest.getResName(), resourceSetRequest);
        }
    }

    private static String getConcatenatedSecurityGroups(List<String> securityGroups) {
        return String.join(":", securityGroups);
    }
}
