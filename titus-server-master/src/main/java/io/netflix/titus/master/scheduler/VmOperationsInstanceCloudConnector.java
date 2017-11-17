package io.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.util.tuple.Either;
import io.netflix.titus.master.scheduler.VMOperations.AgentInfo;
import rx.Completable;
import rx.Observable;

@Singleton
public class VmOperationsInstanceCloudConnector implements InstanceCloudConnector {
    private static final String UNKNOWN_INSTANCE_GROUP = "unknown-instanceGroup";
    private static final String UNKNOWN_INSTANCE_ID = "unknown-instanceId";
    private static final String UNKNOWN_INSTANCE_TYPE = "unknown-instanceType";
    private static final String LAUNCH_CONFIGURATION_SUFFIX = "-launchConfigurationName";

    private final VMOperations vmOperations;

    @Inject
    public VmOperationsInstanceCloudConnector(VMOperations vmOperations) {
        this.vmOperations = vmOperations;
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups() {
        return Observable.fromCallable(this::internalGetInstanceGroups);
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups(List<String> instanceGroupIds) {
        if (instanceGroupIds.isEmpty()) {
            return Observable.just(Collections.emptyList());
        }
        return Observable.fromCallable(() -> internalGetInstanceGroups().stream()
                .filter(instanceGroup -> instanceGroupIds.contains(instanceGroup.getId()))
                .collect(Collectors.toList()));
    }

    @Override
    public Observable<List<InstanceLaunchConfiguration>> getInstanceLaunchConfiguration(List<String> launchConfigurationIds) {
        return Observable.fromCallable(this::internalGetLaunchConfiguration);
    }

    @Override
    public ResourceDimension getInstanceTypeResourceDimension(String instanceType) {
        return ResourceDimension.newBuilder().build();
    }

    @Override
    public Observable<List<Instance>> getInstances(List<String> instanceIds) {
        if (instanceIds.isEmpty()) {
            return Observable.just(Collections.emptyList());
        }
        return Observable.fromCallable(() -> internalGetInstances().stream()
                .filter(instance -> instanceIds.contains(instance.getId()))
                .collect(Collectors.toList()));
    }

    @Override
    public Observable<List<Instance>> getInstancesByInstanceGroupId(String instanceGroupId) {
        if (Strings.isNullOrEmpty(instanceGroupId)) {
            return Observable.just(Collections.emptyList());
        }
        return Observable.fromCallable(() -> internalGetInstances().stream()
                .filter(instance -> instance.getInstanceGroupId().equals(instanceGroupId))
                .collect(Collectors.toList()));
    }

    @Override
    public Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired) {
        return Completable.complete();
    }

    @Override
    public Completable scaleUp(String instanceGroupId, int scaleUpCount) {
        return Completable.complete();
    }

    @Override
    public Observable<List<Either<Boolean, Throwable>>> terminateInstances(String instanceGroup, List<String> instanceIds, boolean shrink) {
        return Observable.empty();
    }

    private List<InstanceGroup> internalGetInstanceGroups() {
        List<InstanceGroup> instanceGroups = new ArrayList<>();
        Multimap<String, String> instanceIdsByInstanceGroupId = ArrayListMultimap.create();

        for (AgentInfo agentInfo : vmOperations.getAgentInfos()) {
            Map<String, String> attributes = agentInfo.getAttributes();
            String instanceGroupId = attributes.getOrDefault("asg", UNKNOWN_INSTANCE_GROUP);
            String instanceId = attributes.getOrDefault("id", agentInfo.getName());
            instanceIdsByInstanceGroupId.put(instanceGroupId, instanceId);
        }

        for (String instanceGroupId : instanceIdsByInstanceGroupId.keySet()) {
            List<String> instanceIds = new ArrayList<>(instanceIdsByInstanceGroupId.get(instanceGroupId));
            int numberOfInstances = instanceIds.size();
            InstanceGroup instanceGroup = InstanceGroup.newBuilder()
                    .withId(instanceGroupId)
                    .withInstanceIds(instanceIds)
                    .withAttributes(new HashMap<>())
                    .withMin(0)
                    .withDesired(numberOfInstances)
                    .withMax(numberOfInstances)
                    .withLaunchConfigurationName(instanceGroupId + "-launchConfigurationName")
                    .withIsLaunchSuspended(true)
                    .withIsTerminateSuspended(true)
                    .build();
            instanceGroups.add(instanceGroup);
        }

        return instanceGroups;
    }

    private List<InstanceLaunchConfiguration> internalGetLaunchConfiguration() {
        Map<String, InstanceLaunchConfiguration> instanceLaunchConfigurations = new HashMap<>();

        for (AgentInfo agentInfo : vmOperations.getAgentInfos()) {
            Map<String, String> attributes = agentInfo.getAttributes();
            String instanceGroupId = attributes.getOrDefault("asg", UNKNOWN_INSTANCE_GROUP);
            String launchConfigurationName = instanceGroupId + LAUNCH_CONFIGURATION_SUFFIX;
            String instanceType = attributes.getOrDefault("itype", UNKNOWN_INSTANCE_TYPE);
            instanceLaunchConfigurations.computeIfAbsent(instanceGroupId, k -> new InstanceLaunchConfiguration(launchConfigurationName, instanceType));
        }

        return new ArrayList<>(instanceLaunchConfigurations.values());
    }

    private List<Instance> internalGetInstances() {
        return vmOperations.getAgentInfos().stream().map(agentInfo -> {
            Map<String, String> attributes = agentInfo.getAttributes();
            String instanceGroupId = attributes.getOrDefault("asg", UNKNOWN_INSTANCE_GROUP);
            String instanceId = attributes.getOrDefault("id", UNKNOWN_INSTANCE_ID);
            return Instance.newBuilder()
                    .withId(instanceId)
                    .withInstanceGroupId(instanceGroupId)
                    .withInstanceState(Instance.InstanceState.Running)
                    .withHostname(agentInfo.getName())
                    .withIpAddress(agentInfo.getName())
                    .withAttributes(new HashMap<>())
                    .build();
        }).collect(Collectors.toList());
    }
}
