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

package com.netflix.titus.runtime.endpoint.v3.grpc;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.CapacityAttributes;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.Owner;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.AvailabilityPercentageLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.ContainerHealthProvider;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.Day;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.HourlyTimeWindow;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.PercentagePerHourDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RatePerIntervalDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RatePercentagePerIntervalDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RelocationLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.TimeWindow;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnhealthyTasksLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnlimitedDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolumeUtils;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.migration.MigrationDetails;
import com.netflix.titus.api.jobmanager.model.job.migration.MigrationPolicy;
import com.netflix.titus.api.jobmanager.model.job.migration.SelfManagedMigrationPolicy;
import com.netflix.titus.api.jobmanager.model.job.migration.SystemDefaultMigrationPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.RetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressAllocation;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressLocation;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.AddressAllocation;
import com.netflix.titus.grpc.protogen.AddressLocation;
import com.netflix.titus.grpc.protogen.BatchJobSpec;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Constraints;
import com.netflix.titus.grpc.protogen.JobCapacityWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor.JobSpecCase;
import com.netflix.titus.grpc.protogen.JobDisruptionBudget;
import com.netflix.titus.grpc.protogen.LogLocation;
import com.netflix.titus.grpc.protogen.MountPerm;
import com.netflix.titus.grpc.protogen.SecurityProfile;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.grpc.protogen.SignedAddressAllocation;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_EVICTION_RESUBMIT_NUMBER;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_NETWORK_INTERFACE_INDEX;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_RESUBMIT_NUMBER;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_SYSTEM_RESUBMIT_NUMBER;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_TASK_INDEX;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_TASK_ORIGINAL_ID;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_TASK_RESUBMIT_OF;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_S3_ACCOUNT_ID;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_S3_ACCOUNT_NAME;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_S3_BUCKET_NAME;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_S3_KEY;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_S3_REGION;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_UI_LOCATION;
import static com.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;
import static com.netflix.titus.common.util.Evaluators.acceptNotNull;
import static com.netflix.titus.common.util.Evaluators.applyNotNull;
import static com.netflix.titus.common.util.StringExt.nonNull;
import static com.netflix.titus.grpc.protogen.Day.Friday;
import static com.netflix.titus.grpc.protogen.Day.Monday;
import static com.netflix.titus.grpc.protogen.Day.Saturday;
import static com.netflix.titus.grpc.protogen.Day.Sunday;
import static com.netflix.titus.grpc.protogen.Day.Thursday;
import static com.netflix.titus.grpc.protogen.Day.Tuesday;
import static com.netflix.titus.grpc.protogen.Day.Wednesday;

public final class GrpcJobManagementModelConverters {

    private GrpcJobManagementModelConverters() {
    }

    public static Job toCoreJob(com.netflix.titus.grpc.protogen.Job grpcJob) {
        return JobModel.newJob()
                .withId(grpcJob.getId())
                .withJobDescriptor(toCoreJobDescriptor(grpcJob.getJobDescriptor()))
                .withStatus(toCoreJobStatus(grpcJob.getStatus()))
                .withStatusHistory(grpcJob.getStatusHistoryList().stream().map(GrpcJobManagementModelConverters::toCoreJobStatus).collect(Collectors.toList()))
                .build();
    }

    public static JobStatus toCoreJobStatus(com.netflix.titus.grpc.protogen.JobStatus grpcJobStatus) {
        return JobModel.newJobStatus()
                .withState(toCoreJobState(grpcJobStatus.getState()))
                .withReasonCode(grpcJobStatus.getReasonCode())
                .withReasonMessage(grpcJobStatus.getReasonMessage())
                .withTimestamp(grpcJobStatus.getTimestamp())
                .build();
    }

    public static JobState toCoreJobState(com.netflix.titus.grpc.protogen.JobStatus.JobState grpcState) {
        switch (grpcState) {
            case Accepted:
                return JobState.Accepted;
            case KillInitiated:
                return JobState.KillInitiated;
            case Finished:
                return JobState.Finished;
        }
        throw new IllegalStateException("Unrecognized GRPC JobState " + grpcState);
    }

    public static JobDescriptor toCoreJobDescriptor(com.netflix.titus.grpc.protogen.JobDescriptor grpcJobDescriptor) {

        JobDescriptor coreJobDescriptor = JobDescriptor.newBuilder()
                .withOwner(toCoreOwner(grpcJobDescriptor.getOwner()))
                .withApplicationName(grpcJobDescriptor.getApplicationName())
                .withJobGroupInfo(toCoreJobGroupInfo(grpcJobDescriptor.getJobGroupInfo()))
                .withCapacityGroup(grpcJobDescriptor.getCapacityGroup())
                .withContainer(toCoreContainer(grpcJobDescriptor.getContainer()))
                .withAttributes(grpcJobDescriptor.getAttributesMap())
                .withDisruptionBudget(toCoreDisruptionBudget(grpcJobDescriptor.getDisruptionBudget()))
                .withExtensions(toCoreJobExtensions(grpcJobDescriptor))
                .build();

        return mapAttributesToCoreJobDescriptor(coreJobDescriptor);
    }

    /*
     * Maps configurations that exist in job attributes to core model types.
     */
    public static <E extends JobDescriptor.JobDescriptorExt> JobDescriptor<E> mapAttributesToCoreJobDescriptor(JobDescriptor<E> coreJobDescriptor) {

        // Extract/map EBS volume attributes
        List<EbsVolume> ebsVolumes = EbsVolumeUtils.getEbsVolumes(coreJobDescriptor);

        return coreJobDescriptor.toBuilder()
                .withContainer(coreJobDescriptor.getContainer().toBuilder()
                        .withContainerResources(coreJobDescriptor.getContainer().getContainerResources().toBuilder()
                                .withEbsVolumes(ebsVolumes)
                                .build())
                        .build())
                .build();
    }

    public static Owner toCoreOwner(com.netflix.titus.grpc.protogen.Owner grpcOwner) {
        return JobModel.newOwner().withTeamEmail(grpcOwner.getTeamEmail()).build();
    }

    public static JobGroupInfo toCoreJobGroupInfo(com.netflix.titus.grpc.protogen.JobGroupInfo grpcJobGroupInfo) {
        return JobModel.newJobGroupInfo()
                .withStack(grpcJobGroupInfo.getStack())
                .withDetail(grpcJobGroupInfo.getDetail())
                .withSequence(grpcJobGroupInfo.getSequence())
                .build();
    }

    public static Image toCoreImage(com.netflix.titus.grpc.protogen.Image grpcImage) {
        return JobModel.newImage()
                .withName(grpcImage.getName())
                .withTag(grpcImage.getTag())
                .withDigest(grpcImage.getDigest())
                .build();
    }

    public static EfsMount toCoreEfsMount(com.netflix.titus.grpc.protogen.ContainerResources.EfsMount grpcEfsMount) {
        return EfsMount.newBuilder()
                .withEfsId(grpcEfsMount.getEfsId())
                .withMountPerm(EfsMount.MountPerm.valueOf(grpcEfsMount.getMountPerm().name()))
                .withMountPoint(grpcEfsMount.getMountPoint())
                .withEfsRelativeMountPoint(grpcEfsMount.getEfsRelativeMountPoint())
                .build();
    }

    private static IpAddressLocation toCoreIpAddressLocation(AddressLocation grpcAddressLocation) {
        return IpAddressLocation.newBuilder()
                .withAvailabilityZone(grpcAddressLocation.getAvailabilityZone())
                .withRegion(grpcAddressLocation.getRegion())
                .withSubnetId(grpcAddressLocation.getSubnetId())
                .build();
    }

    private static IpAddressAllocation toCoreIpAddressAllocation(AddressAllocation grpcAddressAllocation) {
        return IpAddressAllocation.newBuilder()
                .withUuid(grpcAddressAllocation.getUuid())
                .withIpAddressLocation(toCoreIpAddressLocation(grpcAddressAllocation.getAddressLocation()))
                .withIpAddress(grpcAddressAllocation.getAddress())
                .build();
    }

    public static SignedIpAddressAllocation toCoreSignedIpAddressAllocation(SignedAddressAllocation grpcSignedIpAddressAllocation) {
        return SignedIpAddressAllocation.newBuilder()
                .withIpAddressAllocation(toCoreIpAddressAllocation(grpcSignedIpAddressAllocation.getAddressAllocation()))
                .withAuthoritativePublicKey(grpcSignedIpAddressAllocation.getAuthoritativePublicKey().toByteArray())
                .withHostPublicKey(grpcSignedIpAddressAllocation.getHostPublicKey().toByteArray())
                .withHostPublicKeySignature(grpcSignedIpAddressAllocation.getHostPublicKeySignature().toByteArray())
                .withMessage(grpcSignedIpAddressAllocation.getMessage().toByteArray())
                .withMessageSignature(grpcSignedIpAddressAllocation.getMessageSignature().toByteArray())
                .build();
    }

    public static ContainerResources toCoreScalarResources(com.netflix.titus.grpc.protogen.ContainerResources grpcResources) {
        List<EfsMount> coreEfsMounts = grpcResources.getEfsMountsCount() == 0
                ? Collections.emptyList()
                : grpcResources.getEfsMountsList().stream().map(GrpcJobManagementModelConverters::toCoreEfsMount).collect(Collectors.toList());

        List<SignedIpAddressAllocation> signedIpAddressAllocations = grpcResources.getSignedAddressAllocationsCount() == 0
                ? Collections.emptyList()
                : grpcResources.getSignedAddressAllocationsList().stream().map(GrpcJobManagementModelConverters::toCoreSignedIpAddressAllocation)
                .collect(Collectors.toList());

        return JobModel.newContainerResources()
                .withCpu(grpcResources.getCpu())
                .withGpu(grpcResources.getGpu())
                .withMemoryMB(grpcResources.getMemoryMB())
                .withDiskMB(grpcResources.getDiskMB())
                .withNetworkMbps(grpcResources.getNetworkMbps())
                .withAllocateIP(grpcResources.getAllocateIP())
                .withEfsMounts(coreEfsMounts)
                .withShmMB(grpcResources.getShmSizeMB())
                .withSignedIpAddressAllocations(signedIpAddressAllocations)
                .build();
    }

    public static com.netflix.titus.api.jobmanager.model.job.SecurityProfile toCoreSecurityProfile(SecurityProfile grpcSecurityProfile) {
        return JobModel.newSecurityProfile()
                .withSecurityGroups(grpcSecurityProfile.getSecurityGroupsList())
                .withIamRole(grpcSecurityProfile.getIamRole())
                .withAttributes(grpcSecurityProfile.getAttributesMap())
                .build();
    }

    public static Map<String, String> toCoreConstraints(Constraints grpcConstraints) {
        return grpcConstraints.getConstraintsMap();
    }

    private static RetryPolicy toCoreRetryPolicy(com.netflix.titus.grpc.protogen.RetryPolicy grpcRetryPolicy) {
        switch (grpcRetryPolicy.getPolicyCase()) {
            case IMMEDIATE:
                return JobModel.newImmediateRetryPolicy()
                        .withRetries(grpcRetryPolicy.getImmediate().getRetries()).build();
            case DELAYED:
                return JobModel.newDelayedRetryPolicy()
                        .withDelay(grpcRetryPolicy.getDelayed().getDelayMs(), TimeUnit.MILLISECONDS)
                        .withRetries(grpcRetryPolicy.getDelayed().getRetries())
                        .build();
            case EXPONENTIALBACKOFF:
                return JobModel.newExponentialBackoffRetryPolicy()
                        .withInitialDelayMs(grpcRetryPolicy.getExponentialBackOff().getInitialDelayMs())
                        .withMaxDelayMs(grpcRetryPolicy.getExponentialBackOff().getMaxDelayIntervalMs())
                        .withRetries(grpcRetryPolicy.getExponentialBackOff().getRetries())
                        .build();
            default:
                throw new IllegalArgumentException("Unknown retry policy " + grpcRetryPolicy.getPolicyCase());
        }
    }

    public static ServiceJobProcesses toCoreServiceJobProcesses(ServiceJobSpec.ServiceJobProcesses serviceJobProcesses) {
        return JobModel.newServiceJobProcesses()
                .withDisableIncreaseDesired(serviceJobProcesses.getDisableIncreaseDesired())
                .withDisableDecreaseDesired(serviceJobProcesses.getDisableDecreaseDesired())
                .build();
    }

    public static ServiceJobSpec.ServiceJobProcesses toGrpcServiceJobProcesses(ServiceJobProcesses serviceJobProcesses) {
        return ServiceJobSpec.ServiceJobProcesses.newBuilder()
                .setDisableDecreaseDesired(serviceJobProcesses.isDisableDecreaseDesired())
                .setDisableIncreaseDesired(serviceJobProcesses.isDisableIncreaseDesired())
                .build();
    }

    private static MigrationPolicy toCoreMigrationPolicy(com.netflix.titus.grpc.protogen.MigrationPolicy grpcMigrationPolicy) {
        switch (grpcMigrationPolicy.getPolicyCase()) {
            case SYSTEMDEFAULT:
                return JobModel.newSystemDefaultMigrationPolicy().build();
            case SELFMANAGED:
                return JobModel.newSelfManagedMigrationPolicy().build();
            default:
                return JobModel.newSystemDefaultMigrationPolicy().build();
        }
    }

    public static Container toCoreContainer(com.netflix.titus.grpc.protogen.Container grpcContainer) {
        return JobModel.newContainer()
                .withImage(toCoreImage(grpcContainer.getImage()))
                .withContainerResources(toCoreScalarResources(grpcContainer.getResources()))
                .withSecurityProfile(toCoreSecurityProfile(grpcContainer.getSecurityProfile()))
                .withEnv(grpcContainer.getEnvMap())
                .withSoftConstraints(toCoreConstraints(grpcContainer.getSoftConstraints()))
                .withHardConstraints(toCoreConstraints(grpcContainer.getHardConstraints()))
                .withEntryPoint(grpcContainer.getEntryPointList())
                .withCommand(grpcContainer.getCommandList())
                .withAttributes(grpcContainer.getAttributesMap())
                .build();
    }

    public static DisruptionBudget toCoreDisruptionBudget(com.netflix.titus.grpc.protogen.JobDisruptionBudget grpcDisruptionBudget) {
        if (JobDisruptionBudget.getDefaultInstance().equals(grpcDisruptionBudget)) {
            return DisruptionBudget.none();
        }

        return DisruptionBudget.newBuilder()
                .withDisruptionBudgetPolicy(toCoreDisruptionBudgetPolicy(grpcDisruptionBudget))
                .withDisruptionBudgetRate(toCoreDisruptionBudgetRate(grpcDisruptionBudget))
                .withTimeWindows(toCoreTimeWindows(grpcDisruptionBudget.getTimeWindowsList()))
                .withContainerHealthProviders(toCoreContainerHealthProviders(grpcDisruptionBudget.getContainerHealthProvidersList()))
                .build();
    }

    public static DisruptionBudgetPolicy toCoreDisruptionBudgetPolicy(JobDisruptionBudget grpcDisruptionBudget) {
        switch (grpcDisruptionBudget.getPolicyCase()) {
            case SELFMANAGED:
                return SelfManagedDisruptionBudgetPolicy.newBuilder()
                        .withRelocationTimeMs(grpcDisruptionBudget.getSelfManaged().getRelocationTimeMs())
                        .build();
            case AVAILABILITYPERCENTAGELIMIT:
                return AvailabilityPercentageLimitDisruptionBudgetPolicy.newBuilder()
                        .withPercentageOfHealthyContainers(grpcDisruptionBudget.getAvailabilityPercentageLimit().getPercentageOfHealthyContainers())
                        .build();
            case UNHEALTHYTASKSLIMIT:
                return UnhealthyTasksLimitDisruptionBudgetPolicy.newBuilder()
                        .withLimitOfUnhealthyContainers(grpcDisruptionBudget.getUnhealthyTasksLimit().getLimitOfUnhealthyContainers())
                        .build();
            case RELOCATIONLIMIT:
                return RelocationLimitDisruptionBudgetPolicy.newBuilder()
                        .withLimit(grpcDisruptionBudget.getRelocationLimit().getLimit())
                        .build();
            default:
                return SelfManagedDisruptionBudgetPolicy.newBuilder()
                        .build();
        }
    }

    public static DisruptionBudgetRate toCoreDisruptionBudgetRate(JobDisruptionBudget grpcDisruptionBudget) {
        switch (grpcDisruptionBudget.getRateCase()) {
            case RATEUNLIMITED:
                return UnlimitedDisruptionBudgetRate.newBuilder()
                        .build();
            case RATEPERCENTAGEPERHOUR:
                return PercentagePerHourDisruptionBudgetRate.newBuilder()
                        .withMaxPercentageOfContainersRelocatedInHour(grpcDisruptionBudget.getRatePercentagePerHour().getMaxPercentageOfContainersRelocatedInHour())
                        .build();
            case RATEPERINTERVAL:
                return RatePerIntervalDisruptionBudgetRate.newBuilder()
                        .withIntervalMs(grpcDisruptionBudget.getRatePerInterval().getIntervalMs())
                        .withLimitPerInterval(grpcDisruptionBudget.getRatePerInterval().getLimitPerInterval())
                        .build();
            case RATEPERCENTAGEPERINTERVAL:
                return RatePercentagePerIntervalDisruptionBudgetRate.newBuilder()
                        .withIntervalMs(grpcDisruptionBudget.getRatePercentagePerInterval().getIntervalMs())
                        .withPercentageLimitPerInterval(grpcDisruptionBudget.getRatePercentagePerInterval().getPercentageLimitPerInterval())
                        .build();
            default:
                return UnlimitedDisruptionBudgetRate.newBuilder()
                        .build();
        }
    }

    public static List<TimeWindow> toCoreTimeWindows(List<com.netflix.titus.grpc.protogen.TimeWindow> grpcTimeWindows) {
        return grpcTimeWindows.stream().map(GrpcJobManagementModelConverters::toCoreTimeWindow).collect(Collectors.toList());
    }

    public static TimeWindow toCoreTimeWindow(com.netflix.titus.grpc.protogen.TimeWindow grpcTimeWindow) {
        return TimeWindow.newBuilder()
                .withDays(toCoreDays(grpcTimeWindow.getDaysList()))
                .withHourlyTimeWindows(toCoreHourlyTimeWindows(grpcTimeWindow.getHourlyTimeWindowsList()))
                .withTimeZone(grpcTimeWindow.getTimeZone())
                .build();
    }

    public static List<Day> toCoreDays(List<com.netflix.titus.grpc.protogen.Day> grpcDays) {
        return grpcDays.stream().map(GrpcJobManagementModelConverters::toCoreDay).collect(Collectors.toList());
    }

    public static Day toCoreDay(com.netflix.titus.grpc.protogen.Day grpcDay) {
        switch (grpcDay) {
            case Monday:
                return Day.Monday;
            case Tuesday:
                return Day.Tuesday;
            case Wednesday:
                return Day.Wednesday;
            case Thursday:
                return Day.Thursday;
            case Friday:
                return Day.Friday;
            case Saturday:
                return Day.Saturday;
            case Sunday:
                return Day.Sunday;
            default:
                throw new IllegalArgumentException("Unknown day: " + grpcDay);
        }
    }

    public static List<HourlyTimeWindow> toCoreHourlyTimeWindows(List<com.netflix.titus.grpc.protogen.TimeWindow.HourlyTimeWindow> grpcHourlyTimeWindows) {
        return grpcHourlyTimeWindows.stream().map(GrpcJobManagementModelConverters::toCoreHourlyTimeWindow).collect(Collectors.toList());
    }

    public static HourlyTimeWindow toCoreHourlyTimeWindow(com.netflix.titus.grpc.protogen.TimeWindow.HourlyTimeWindow grpcHourlyTimeWindow) {
        return HourlyTimeWindow.newBuilder()
                .withStartHour(grpcHourlyTimeWindow.getStartHour())
                .withEndHour(grpcHourlyTimeWindow.getEndHour())
                .build();
    }

    public static List<ContainerHealthProvider> toCoreContainerHealthProviders(List<com.netflix.titus.grpc.protogen.ContainerHealthProvider> grpcContainerHealthProviders) {
        return grpcContainerHealthProviders.stream().map(GrpcJobManagementModelConverters::toCoreHourlyTimeWindow).collect(Collectors.toList());
    }

    public static ContainerHealthProvider toCoreHourlyTimeWindow(com.netflix.titus.grpc.protogen.ContainerHealthProvider grpcContainerHealthProvider) {
        return ContainerHealthProvider.newBuilder()
                .withName(grpcContainerHealthProvider.getName())
                .withAttributes(grpcContainerHealthProvider.getAttributesMap())
                .build();
    }

    public static JobDescriptor.JobDescriptorExt toCoreJobExtensions(com.netflix.titus.grpc.protogen.JobDescriptor grpcJobDescriptor) {
        if (grpcJobDescriptor.getJobSpecCase() == JobSpecCase.BATCH) {
            BatchJobSpec batchSpec = grpcJobDescriptor.getBatch();
            return JobModel.newBatchJobExt()
                    .withSize(batchSpec.getSize())
                    .withRetryPolicy(toCoreRetryPolicy(batchSpec.getRetryPolicy()))
                    .withRuntimeLimitMs(batchSpec.getRuntimeLimitSec() * 1000)
                    .withRetryOnRuntimeLimit(batchSpec.getRetryOnRuntimeLimit())
                    .build();
        }
        ServiceJobSpec serviceSpec = grpcJobDescriptor.getService();
        return JobModel.newServiceJobExt()
                .withCapacity(toCoreCapacity(serviceSpec.getCapacity()))
                .withRetryPolicy(toCoreRetryPolicy(serviceSpec.getRetryPolicy()))
                .withMigrationPolicy(toCoreMigrationPolicy(serviceSpec.getMigrationPolicy()))
                .withEnabled(serviceSpec.getEnabled())
                .withServiceJobProcesses(toCoreServiceJobProcesses(serviceSpec.getServiceJobProcesses()))
                .build();
    }

    public static com.netflix.titus.api.jobmanager.model.job.Capacity toCoreCapacity(Capacity capacity) {
        return JobModel.newCapacity()
                .withMin(capacity.getMin())
                .withDesired(capacity.getDesired())
                .withMax(capacity.getMax())
                .build();
    }

    public static com.netflix.titus.api.jobmanager.model.job.CapacityAttributes toCoreCapacityAttributes(JobCapacityWithOptionalAttributes capacity) {
        CapacityAttributes.Builder builder = JobModel.newCapacityAttributes();
        Evaluators.acceptIfTrue(capacity.hasDesired(), valueAccepted -> builder.withDesired(capacity.getDesired().getValue()));
        Evaluators.acceptIfTrue(capacity.hasMin(), valueAccepted -> builder.withMin(capacity.getMin().getValue()));
        Evaluators.acceptIfTrue(capacity.hasMax(), valueAccepted -> builder.withMax(capacity.getMax().getValue()));
        return builder.build();
    }

    public static com.netflix.titus.api.jobmanager.model.job.CapacityAttributes toCoreCapacityAttributes(Capacity capacity) {
        return JobModel.newCapacityAttributes()
                .withDesired(capacity.getDesired())
                .withMax(capacity.getMax())
                .withMin(capacity.getMin())
                .build();
    }

    public static Task toCoreTask(com.netflix.titus.grpc.protogen.Task grpcTask) {
        Map<String, String> taskContext = grpcTask.getTaskContextMap();

        String originalId = Preconditions.checkNotNull(
                taskContext.get(TASK_ATTRIBUTES_TASK_ORIGINAL_ID), TASK_ATTRIBUTES_TASK_ORIGINAL_ID + " missing in Task entity"
        );
        String resubmitOf = taskContext.get(TASK_ATTRIBUTES_TASK_RESUBMIT_OF);
        int taskResubmitNumber = Integer.parseInt(taskContext.get(TASK_ATTRIBUTES_RESUBMIT_NUMBER));
        int systemResubmitNumber = Integer.parseInt(grpcTask.getTaskContextMap().getOrDefault(TASK_ATTRIBUTES_SYSTEM_RESUBMIT_NUMBER, "0"));
        int evictionResubmitNumber = Integer.parseInt(grpcTask.getTaskContextMap().getOrDefault(TASK_ATTRIBUTES_EVICTION_RESUBMIT_NUMBER, "0"));

        String taskIndexStr = taskContext.get(TASK_ATTRIBUTES_TASK_INDEX);

        // Based on presence of the task index, we decide if it is batch or service task.
        boolean isBatchTask = taskIndexStr != null;
        Task.TaskBuilder<?, ?> builder = isBatchTask ? JobModel.newBatchJobTask() : JobModel.newServiceJobTask();
        builder.withId(grpcTask.getId())
                .withJobId(grpcTask.getJobId())
                .withStatus(toCoreTaskStatus(grpcTask.getStatus()))
                .withStatusHistory(grpcTask.getStatusHistoryList().stream().map(GrpcJobManagementModelConverters::toCoreTaskStatus).collect(Collectors.toList()))
                .withResubmitNumber(taskResubmitNumber)
                .withOriginalId(originalId)
                .withResubmitOf(resubmitOf)
                .withSystemResubmitNumber(systemResubmitNumber)
                .withEvictionResubmitNumber(evictionResubmitNumber)
                .withTaskContext(taskContext)
                .withAttributes(buildAttributesMapForCoreTask(grpcTask));

        if (isBatchTask) { // Batch job
            ((BatchJobTask.Builder) builder).withIndex(Integer.parseInt(taskIndexStr));
        } else {
            ((ServiceJobTask.Builder) builder).withMigrationDetails(toCoreMigrationDetails(grpcTask.getMigrationDetails()));
        }

        return builder.build();
    }

    private static Map<String, String> buildAttributesMapForCoreTask(com.netflix.titus.grpc.protogen.Task grpcTask) {
        Map<String, String> attributes = new HashMap<>(grpcTask.getAttributesMap());
        if (grpcTask.hasLogLocation()) {
            LogLocation logLocation = grpcTask.getLogLocation();
            if (logLocation != null) {
                if (logLocation.getUi() != null) {
                    attributes.put(TASK_ATTRIBUTE_LOG_UI_LOCATION, logLocation.getUi().getUrl());
                }
                if (logLocation.getS3() != null) {
                    LogLocation.S3 s3 = logLocation.getS3();
                    attributes.put(TASK_ATTRIBUTE_LOG_S3_ACCOUNT_NAME, s3.getAccountName());
                    attributes.put(TASK_ATTRIBUTE_LOG_S3_ACCOUNT_ID, s3.getAccountId());
                    attributes.put(TASK_ATTRIBUTE_LOG_S3_BUCKET_NAME, s3.getBucket());
                    attributes.put(TASK_ATTRIBUTE_LOG_S3_KEY, s3.getKey());
                    attributes.put(TASK_ATTRIBUTE_LOG_S3_REGION, s3.getRegion());
                }
            }
        }
        return attributes;
    }

    public static Task toCoreTask(Job<?> job, com.netflix.titus.grpc.protogen.Task grpcTask) {
        return toCoreTask(grpcTask).toBuilder()
                .withTwoLevelResources(toCoreTwoLevelResources(job, grpcTask))
                .build();
    }

    /**
     * We do not expose the {@link TwoLevelResource} data outside Titus, so we try to reconstruct this information
     * from the GRPC model.
     */
    private static List<TwoLevelResource> toCoreTwoLevelResources(Job<?> job, com.netflix.titus.grpc.protogen.Task grpcTask) {
        Map<String, String> context = grpcTask.getTaskContextMap();

        String eniIndex = context.get(TASK_ATTRIBUTES_NETWORK_INTERFACE_INDEX);
        if (eniIndex == null) {
            return Collections.emptyList();
        }

        TwoLevelResource resource = TwoLevelResource.newBuilder()
                .withIndex(Integer.parseInt(eniIndex))
                .withName("ENIs")
                .withValue(StringExt.concatenate(job.getJobDescriptor().getContainer().getSecurityProfile().getSecurityGroups(), ":"))
                .build();

        return Collections.singletonList(resource);
    }

    public static TaskStatus toCoreTaskStatus(com.netflix.titus.grpc.protogen.TaskStatus grpcStatus) {
        return JobModel.newTaskStatus()
                .withState(toCoreTaskState(grpcStatus.getState()))
                .withReasonCode(grpcStatus.getReasonCode())
                .withReasonMessage(grpcStatus.getReasonMessage())
                .withTimestamp(grpcStatus.getTimestamp())
                .build();
    }

    public static TaskState toCoreTaskState(com.netflix.titus.grpc.protogen.TaskStatus.TaskState grpcState) {
        TaskState state;
        switch (grpcState) {
            case Accepted:
                state = TaskState.Accepted;
                break;
            case Launched:
                state = TaskState.Launched;
                break;
            case StartInitiated:
                state = TaskState.StartInitiated;
                break;
            case Started:
                state = TaskState.Started;
                break;
            case KillInitiated:
                state = TaskState.KillInitiated;
                break;
            case Disconnected:
                state = TaskState.Disconnected;
                break;
            case Finished:
                state = TaskState.Finished;
                break;
            default:
                throw new IllegalArgumentException("Unrecognized task state " + grpcState);
        }
        return state;
    }

    public static com.netflix.titus.grpc.protogen.Owner toGrpcOwner(Owner owner) {
        return com.netflix.titus.grpc.protogen.Owner.newBuilder()
                .setTeamEmail(owner.getTeamEmail())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.JobGroupInfo toGrpcJobGroupInfo(JobGroupInfo jobGroupInfo) {
        if (jobGroupInfo == null) {
            return com.netflix.titus.grpc.protogen.JobGroupInfo.getDefaultInstance();
        }

        com.netflix.titus.grpc.protogen.JobGroupInfo.Builder builder = com.netflix.titus.grpc.protogen.JobGroupInfo.newBuilder();
        acceptNotNull(jobGroupInfo.getStack(), builder::setStack);
        acceptNotNull(jobGroupInfo.getDetail(), builder::setDetail);
        acceptNotNull(jobGroupInfo.getSequence(), builder::setSequence);

        return builder.build();
    }

    private static com.netflix.titus.grpc.protogen.Image toGrpcImage(Image image) {
        com.netflix.titus.grpc.protogen.Image.Builder builder = com.netflix.titus.grpc.protogen.Image.newBuilder();
        builder.setName(image.getName());
        acceptNotNull(image.getTag(), builder::setTag);
        acceptNotNull(image.getDigest(), builder::setDigest);
        return builder.build();
    }

    public static com.netflix.titus.grpc.protogen.ContainerResources.EfsMount toGrpcEfsMount(EfsMount coreEfsMount) {
        com.netflix.titus.grpc.protogen.ContainerResources.EfsMount.Builder builder = com.netflix.titus.grpc.protogen.ContainerResources.EfsMount.newBuilder()
                .setEfsId(coreEfsMount.getEfsId())
                .setMountPoint(coreEfsMount.getMountPoint())
                .setEfsRelativeMountPoint(coreEfsMount.getEfsRelativeMountPoint());
        applyNotNull(coreEfsMount.getMountPerm(), perm -> builder.setMountPerm(MountPerm.valueOf(perm.name())));
        return builder.build();
    }

    public static AddressLocation toGrpcAddressLocation(IpAddressLocation coreIpAddressLocation) {
        return AddressLocation.newBuilder()
                .setAvailabilityZone(coreIpAddressLocation.getAvailabilityZone())
                .setRegion(coreIpAddressLocation.getRegion())
                .setSubnetId(coreIpAddressLocation.getSubnetId())
                .build();
    }

    public static AddressAllocation toGrpcAddressAllocation(IpAddressAllocation coreIpAddressAllocation) {
        return AddressAllocation.newBuilder()
                .setUuid(coreIpAddressAllocation.getAllocationId())
                .setAddress(coreIpAddressAllocation.getIpAddress())
                .setAddressLocation(toGrpcAddressLocation(coreIpAddressAllocation.getIpAddressLocation()))
                .build();
    }

    public static SignedAddressAllocation toGrpcSignedAddressAllocation(SignedIpAddressAllocation coreSignedIpAddressAllocation) {
        return SignedAddressAllocation.newBuilder()
                .setAddressAllocation(toGrpcAddressAllocation(coreSignedIpAddressAllocation.getIpAddressAllocation()))
                .setAuthoritativePublicKey(ByteString.copyFrom(coreSignedIpAddressAllocation.getAuthoritativePublicKey()))
                .setHostPublicKey(ByteString.copyFrom(coreSignedIpAddressAllocation.getHostPublicKey()))
                .setHostPublicKeySignature(ByteString.copyFrom(coreSignedIpAddressAllocation.getHostPublicKeySignature()))
                .setMessage(ByteString.copyFrom(coreSignedIpAddressAllocation.getMessage()))
                .setMessageSignature(ByteString.copyFrom(coreSignedIpAddressAllocation.getMessageSignature()))
                .build();
    }

    public static com.netflix.titus.grpc.protogen.ContainerResources toGrpcResources(ContainerResources containerResources) {
        List<com.netflix.titus.grpc.protogen.ContainerResources.EfsMount> grpcEfsMounts = containerResources.getEfsMounts().isEmpty()
                ? Collections.emptyList()
                : containerResources.getEfsMounts().stream().map(GrpcJobManagementModelConverters::toGrpcEfsMount).collect(Collectors.toList());
        List<SignedAddressAllocation> grpcSignedAddressAllocation = containerResources.getSignedIpAddressAllocations().isEmpty()
                ? Collections.emptyList()
                : containerResources.getSignedIpAddressAllocations().stream().map(GrpcJobManagementModelConverters::toGrpcSignedAddressAllocation)
                .collect(Collectors.toList());
        return com.netflix.titus.grpc.protogen.ContainerResources.newBuilder()
                .setCpu(containerResources.getCpu())
                .setGpu(containerResources.getGpu())
                .setMemoryMB(containerResources.getMemoryMB())
                .setDiskMB(containerResources.getDiskMB())
                .setNetworkMbps(containerResources.getNetworkMbps())
                .setAllocateIP(containerResources.isAllocateIP())
                .addAllEfsMounts(grpcEfsMounts)
                .setShmSizeMB(containerResources.getShmMB())
                .addAllSignedAddressAllocations(grpcSignedAddressAllocation)
                .build();
    }

    public static SecurityProfile toGrpcSecurityProfile(com.netflix.titus.api.jobmanager.model.job.SecurityProfile coreSecurityProfile) {
        return SecurityProfile.newBuilder()
                .addAllSecurityGroups(coreSecurityProfile.getSecurityGroups())
                .setIamRole(nonNull(coreSecurityProfile.getIamRole()))
                .putAllAttributes(coreSecurityProfile.getAttributes())
                .build();
    }

    public static Constraints toGrpcConstraints(Map<String, String> constraints) {
        if (constraints.isEmpty()) {
            return Constraints.getDefaultInstance();
        }
        return Constraints.newBuilder().putAllConstraints(constraints).build();
    }

    public static com.netflix.titus.grpc.protogen.Container toGrpcContainer(Container container) {
        return com.netflix.titus.grpc.protogen.Container.newBuilder()
                .setImage(toGrpcImage(container.getImage()))
                .setResources(toGrpcResources(container.getContainerResources()))
                .setSecurityProfile(toGrpcSecurityProfile(container.getSecurityProfile()))
                .putAllAttributes(container.getAttributes())
                .setSoftConstraints(toGrpcConstraints(container.getSoftConstraints()))
                .setHardConstraints(toGrpcConstraints(container.getHardConstraints()))
                .addAllEntryPoint(container.getEntryPoint())
                .addAllCommand(container.getCommand())
                .putAllEnv(container.getEnv())
                .build();
    }

    private static com.netflix.titus.grpc.protogen.RetryPolicy toGrpcRetryPolicy(RetryPolicy retryPolicy) {
        com.netflix.titus.grpc.protogen.RetryPolicy.Builder builder = com.netflix.titus.grpc.protogen.RetryPolicy.newBuilder();
        if (retryPolicy instanceof ImmediateRetryPolicy) {
            builder.setImmediate(com.netflix.titus.grpc.protogen.RetryPolicy.Immediate.newBuilder().setRetries(retryPolicy.getRetries()));
        } else if (retryPolicy instanceof DelayedRetryPolicy) {
            DelayedRetryPolicy delayed = (DelayedRetryPolicy) retryPolicy;
            builder.setDelayed(com.netflix.titus.grpc.protogen.RetryPolicy.Delayed.newBuilder()
                    .setRetries(retryPolicy.getRetries())
                    .setDelayMs(delayed.getDelayMs())
            );
        } else if (retryPolicy instanceof ExponentialBackoffRetryPolicy) {
            ExponentialBackoffRetryPolicy exponential = (ExponentialBackoffRetryPolicy) retryPolicy;
            builder.setExponentialBackOff(com.netflix.titus.grpc.protogen.RetryPolicy.ExponentialBackOff.newBuilder()
                    .setInitialDelayMs(exponential.getInitialDelayMs())
                    .setMaxDelayIntervalMs(exponential.getMaxDelayMs())
                    .setRetries(retryPolicy.getRetries())
            );
        } else {
            throw new IllegalStateException("Unknown retry policy " + retryPolicy);
        }
        return builder.build();
    }

    private static com.netflix.titus.grpc.protogen.MigrationPolicy toGrpcMigrationPolicy(MigrationPolicy migrationPolicy) {
        com.netflix.titus.grpc.protogen.MigrationPolicy.Builder builder = com.netflix.titus.grpc.protogen.MigrationPolicy.newBuilder();
        if (migrationPolicy instanceof SystemDefaultMigrationPolicy) {
            builder.setSystemDefault(com.netflix.titus.grpc.protogen.MigrationPolicy.SystemDefault.newBuilder());
        } else if (migrationPolicy instanceof SelfManagedMigrationPolicy) {
            builder.setSelfManaged(com.netflix.titus.grpc.protogen.MigrationPolicy.SelfManaged.newBuilder());
        } else {
            builder.setSystemDefault(com.netflix.titus.grpc.protogen.MigrationPolicy.SystemDefault.newBuilder());
        }
        return builder.build();
    }

    public static BatchJobSpec toGrpcBatchSpec(BatchJobExt batchJobExt) {
        int desired = batchJobExt.getSize();
        return BatchJobSpec.newBuilder()
                .setSize(desired)
                .setRuntimeLimitSec(batchJobExt.getRuntimeLimitMs() / 1000)
                .setRetryPolicy(toGrpcRetryPolicy(batchJobExt.getRetryPolicy()))
                .setRetryOnRuntimeLimit(batchJobExt.isRetryOnRuntimeLimit())
                .build();
    }

    public static ServiceJobSpec toGrpcServiceSpec(ServiceJobExt serviceJobExt) {
        com.netflix.titus.api.jobmanager.model.job.Capacity capacity = serviceJobExt.getCapacity();
        ServiceJobSpec.Builder builder = ServiceJobSpec.newBuilder()
                .setServiceJobProcesses(toGrpcServiceJobProcesses(serviceJobExt.getServiceJobProcesses()))
                .setEnabled(serviceJobExt.isEnabled())
                .setCapacity(toGrpcCapacity(capacity))
                .setRetryPolicy(toGrpcRetryPolicy(serviceJobExt.getRetryPolicy()))
                .setMigrationPolicy(toGrpcMigrationPolicy(serviceJobExt.getMigrationPolicy()));
        return builder.build();
    }

    public static Capacity toGrpcCapacity(com.netflix.titus.api.jobmanager.model.job.Capacity capacity) {
        return Capacity.newBuilder()
                .setMin(capacity.getMin())
                .setDesired(capacity.getDesired())
                .setMax(capacity.getMax())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.JobDisruptionBudget toGrpcDisruptionBudget(DisruptionBudget coreDisruptionBudget) {
        JobDisruptionBudget.Builder builder = JobDisruptionBudget.newBuilder();
        DisruptionBudgetPolicy disruptionBudgetPolicy = coreDisruptionBudget.getDisruptionBudgetPolicy();
        if (disruptionBudgetPolicy instanceof SelfManagedDisruptionBudgetPolicy) {
            builder.setSelfManaged(JobDisruptionBudget.SelfManaged.newBuilder()
                    .setRelocationTimeMs(((SelfManagedDisruptionBudgetPolicy) disruptionBudgetPolicy).getRelocationTimeMs()));
        } else if (disruptionBudgetPolicy instanceof AvailabilityPercentageLimitDisruptionBudgetPolicy) {
            builder.setAvailabilityPercentageLimit(JobDisruptionBudget.AvailabilityPercentageLimit.newBuilder()
                    .setPercentageOfHealthyContainers(((AvailabilityPercentageLimitDisruptionBudgetPolicy) disruptionBudgetPolicy).getPercentageOfHealthyContainers()));
        } else if (disruptionBudgetPolicy instanceof UnhealthyTasksLimitDisruptionBudgetPolicy) {
            builder.setUnhealthyTasksLimit(JobDisruptionBudget.UnhealthyTasksLimit.newBuilder()
                    .setLimitOfUnhealthyContainers(((UnhealthyTasksLimitDisruptionBudgetPolicy) disruptionBudgetPolicy).getLimitOfUnhealthyContainers()));
        } else if (disruptionBudgetPolicy instanceof RelocationLimitDisruptionBudgetPolicy) {
            builder.setRelocationLimit(JobDisruptionBudget.RelocationLimit.newBuilder()
                    .setLimit(((RelocationLimitDisruptionBudgetPolicy) disruptionBudgetPolicy).getLimit()));
        }

        DisruptionBudgetRate disruptionBudgetRate = coreDisruptionBudget.getDisruptionBudgetRate();
        if (disruptionBudgetRate instanceof UnlimitedDisruptionBudgetRate) {
            builder.setRateUnlimited(JobDisruptionBudget.RateUnlimited.newBuilder().build());
        } else if (disruptionBudgetRate instanceof PercentagePerHourDisruptionBudgetRate) {
            builder.setRatePercentagePerHour(JobDisruptionBudget.RatePercentagePerHour.newBuilder()
                    .setMaxPercentageOfContainersRelocatedInHour(((PercentagePerHourDisruptionBudgetRate) disruptionBudgetRate).getMaxPercentageOfContainersRelocatedInHour()));
        } else if (disruptionBudgetRate instanceof RatePerIntervalDisruptionBudgetRate) {
            RatePerIntervalDisruptionBudgetRate ratePerInterval = (RatePerIntervalDisruptionBudgetRate) disruptionBudgetRate;
            builder.setRatePerInterval(JobDisruptionBudget.RatePerInterval.newBuilder()
                    .setIntervalMs(ratePerInterval.getIntervalMs())
                    .setLimitPerInterval(ratePerInterval.getLimitPerInterval())
                    .build()
            );
        } else if (disruptionBudgetRate instanceof RatePercentagePerIntervalDisruptionBudgetRate) {
            RatePercentagePerIntervalDisruptionBudgetRate ratePercentagePerInterval = (RatePercentagePerIntervalDisruptionBudgetRate) disruptionBudgetRate;
            builder.setRatePercentagePerInterval(JobDisruptionBudget.RatePercentagePerInterval.newBuilder()
                    .setIntervalMs(ratePercentagePerInterval.getIntervalMs())
                    .setPercentageLimitPerInterval(ratePercentagePerInterval.getPercentageLimitPerInterval())
                    .build()
            );
        }

        return builder
                .addAllTimeWindows(toGrpcTimeWindows(coreDisruptionBudget.getTimeWindows()))
                .addAllContainerHealthProviders(toGrpcContainerHealthProviders(coreDisruptionBudget.getContainerHealthProviders()))
                .build();
    }

    public static List<com.netflix.titus.grpc.protogen.TimeWindow> toGrpcTimeWindows(List<TimeWindow> grpcTimeWindows) {
        return grpcTimeWindows.stream().map(GrpcJobManagementModelConverters::toGrpcTimeWindow).collect(Collectors.toList());
    }

    public static com.netflix.titus.grpc.protogen.TimeWindow toGrpcTimeWindow(TimeWindow coreTimeWindow) {
        return com.netflix.titus.grpc.protogen.TimeWindow.newBuilder()
                .addAllDays(toGrpcDays(coreTimeWindow.getDays()))
                .addAllHourlyTimeWindows(toGrpcHourlyTimeWindows(coreTimeWindow.getHourlyTimeWindows()))
                .setTimeZone(coreTimeWindow.getTimeZone())
                .build();
    }

    public static List<com.netflix.titus.grpc.protogen.Day> toGrpcDays(List<Day> coreDays) {
        return coreDays.stream().map(GrpcJobManagementModelConverters::toGrpcDay).collect(Collectors.toList());
    }

    public static com.netflix.titus.grpc.protogen.Day toGrpcDay(Day coreDay) {
        switch (coreDay) {
            case Monday:
                return Monday;
            case Tuesday:
                return Tuesday;
            case Wednesday:
                return Wednesday;
            case Thursday:
                return Thursday;
            case Friday:
                return Friday;
            case Saturday:
                return Saturday;
            case Sunday:
                return Sunday;
            default:
                throw new IllegalArgumentException("Unknown day: " + coreDay);
        }
    }

    public static List<com.netflix.titus.grpc.protogen.TimeWindow.HourlyTimeWindow> toGrpcHourlyTimeWindows(List<HourlyTimeWindow> coreHourlyTimeWindows) {
        return coreHourlyTimeWindows.stream().map(GrpcJobManagementModelConverters::toGrpcHourlyTimeWindow).collect(Collectors.toList());
    }

    public static com.netflix.titus.grpc.protogen.TimeWindow.HourlyTimeWindow toGrpcHourlyTimeWindow(HourlyTimeWindow coreHourlyTimeWindow) {
        return com.netflix.titus.grpc.protogen.TimeWindow.HourlyTimeWindow.newBuilder()
                .setStartHour(coreHourlyTimeWindow.getStartHour())
                .setEndHour(coreHourlyTimeWindow.getEndHour())
                .build();
    }

    public static List<com.netflix.titus.grpc.protogen.ContainerHealthProvider> toGrpcContainerHealthProviders(List<ContainerHealthProvider> grpcContainerHealthProviders) {
        return grpcContainerHealthProviders.stream().map(GrpcJobManagementModelConverters::toGrpcHourlyTimeWindow).collect(Collectors.toList());
    }

    public static com.netflix.titus.grpc.protogen.ContainerHealthProvider toGrpcHourlyTimeWindow(ContainerHealthProvider coreContainerHealthProvider) {
        return com.netflix.titus.grpc.protogen.ContainerHealthProvider.newBuilder()
                .setName(coreContainerHealthProvider.getName())
                .putAllAttributes(coreContainerHealthProvider.getAttributes())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.JobDescriptor toGrpcJobDescriptor(JobDescriptor<?> jobDescriptor) {
        com.netflix.titus.grpc.protogen.JobDescriptor.Builder builder = com.netflix.titus.grpc.protogen.JobDescriptor.newBuilder()
                .setOwner(toGrpcOwner(jobDescriptor.getOwner()))
                .setApplicationName(jobDescriptor.getApplicationName())
                .setCapacityGroup(jobDescriptor.getCapacityGroup())
                .setContainer(toGrpcContainer(jobDescriptor.getContainer()))
                .setJobGroupInfo(toGrpcJobGroupInfo(jobDescriptor.getJobGroupInfo()))
                .setDisruptionBudget(toGrpcDisruptionBudget(jobDescriptor.getDisruptionBudget()))
                .putAllAttributes(jobDescriptor.getAttributes());

        if (jobDescriptor.getExtensions() instanceof BatchJobExt) {
            builder.setBatch(toGrpcBatchSpec((BatchJobExt) jobDescriptor.getExtensions()));
        } else {
            builder.setService(toGrpcServiceSpec((ServiceJobExt) jobDescriptor.getExtensions()));
        }

        return builder.build();
    }

    public static com.netflix.titus.grpc.protogen.JobStatus toGrpcJobStatus(JobStatus status) {
        com.netflix.titus.grpc.protogen.JobStatus.Builder builder = com.netflix.titus.grpc.protogen.JobStatus.newBuilder();
        switch (status.getState()) {
            case Accepted:
                builder.setState(com.netflix.titus.grpc.protogen.JobStatus.JobState.Accepted);
                break;
            case KillInitiated:
                builder.setState(com.netflix.titus.grpc.protogen.JobStatus.JobState.KillInitiated);
                break;
            case Finished:
                builder.setState(com.netflix.titus.grpc.protogen.JobStatus.JobState.Finished);
                break;
            default:
                builder.setState(com.netflix.titus.grpc.protogen.JobStatus.JobState.UNRECOGNIZED);
        }
        applyNotNull(status.getReasonCode(), builder::setReasonCode);
        applyNotNull(status.getReasonMessage(), builder::setReasonMessage);
        builder.setTimestamp(status.getTimestamp());
        return builder.build();
    }

    public static List<com.netflix.titus.grpc.protogen.JobStatus> toGrpcJobStatusHistory(List<JobStatus> statusHistory) {
        if (isNullOrEmpty(statusHistory)) {
            return Collections.emptyList();
        }
        return statusHistory.stream()
                .map(GrpcJobManagementModelConverters::toGrpcJobStatus)
                .collect(Collectors.toList());
    }

    public static com.netflix.titus.grpc.protogen.TaskStatus toGrpcTaskStatus(TaskStatus status) {
        com.netflix.titus.grpc.protogen.TaskStatus.Builder builder = com.netflix.titus.grpc.protogen.TaskStatus.newBuilder();
        switch (status.getState()) {
            case Accepted:
                builder.setState(com.netflix.titus.grpc.protogen.TaskStatus.TaskState.Accepted);
                break;
            case Launched:
                builder.setState(com.netflix.titus.grpc.protogen.TaskStatus.TaskState.Launched);
                break;
            case StartInitiated:
                builder.setState(com.netflix.titus.grpc.protogen.TaskStatus.TaskState.StartInitiated);
                break;
            case Started:
                builder.setState(com.netflix.titus.grpc.protogen.TaskStatus.TaskState.Started);
                break;
            case Disconnected:
                builder.setState(com.netflix.titus.grpc.protogen.TaskStatus.TaskState.Disconnected);
                break;
            case KillInitiated:
                builder.setState(com.netflix.titus.grpc.protogen.TaskStatus.TaskState.KillInitiated);
                break;
            case Finished:
                builder.setState(com.netflix.titus.grpc.protogen.TaskStatus.TaskState.Finished);
                break;
            default:
                builder.setState(com.netflix.titus.grpc.protogen.TaskStatus.TaskState.UNRECOGNIZED);
        }
        applyNotNull(status.getReasonCode(), builder::setReasonCode);
        applyNotNull(status.getReasonMessage(), builder::setReasonMessage);
        builder.setTimestamp(status.getTimestamp());
        return builder.build();
    }

    public static List<com.netflix.titus.grpc.protogen.TaskStatus> toGrpcTaskStatusHistory(List<TaskStatus> statusHistory) {
        if (isNullOrEmpty(statusHistory)) {
            return Collections.emptyList();
        }
        return statusHistory.stream()
                .map(GrpcJobManagementModelConverters::toGrpcTaskStatus)
                .collect(Collectors.toList());
    }

    public static com.netflix.titus.grpc.protogen.Job toGrpcJob(Job<?> coreJob) {
        return com.netflix.titus.grpc.protogen.Job.newBuilder()
                .setId(coreJob.getId())
                .setJobDescriptor(toGrpcJobDescriptor(coreJob.getJobDescriptor()))
                .setStatus(toGrpcJobStatus(coreJob.getStatus()))
                .addAllStatusHistory(toGrpcJobStatusHistory(coreJob.getStatusHistory()))
                .build();
    }

    public static com.netflix.titus.grpc.protogen.Task toGrpcTask(Task coreTask, LogStorageInfo<Task> logStorageInfo) {
        Map<String, String> taskContext = new HashMap<>(coreTask.getTaskContext());
        taskContext.put(TASK_ATTRIBUTES_TASK_ORIGINAL_ID, coreTask.getOriginalId());
        taskContext.put(TASK_ATTRIBUTES_RESUBMIT_NUMBER, Integer.toString(coreTask.getResubmitNumber()));
        taskContext.put(TASK_ATTRIBUTES_SYSTEM_RESUBMIT_NUMBER, Integer.toString(coreTask.getSystemResubmitNumber()));
        taskContext.put(TASK_ATTRIBUTES_EVICTION_RESUBMIT_NUMBER, Integer.toString(coreTask.getEvictionResubmitNumber()));
        coreTask.getResubmitOf().ifPresent(resubmitOf -> taskContext.put(TASK_ATTRIBUTES_TASK_RESUBMIT_OF, resubmitOf));

        if (coreTask instanceof BatchJobTask) {
            BatchJobTask batchTask = (BatchJobTask) coreTask;
            taskContext.put(TASK_ATTRIBUTES_TASK_INDEX, Integer.toString(batchTask.getIndex()));
        }

        com.netflix.titus.grpc.protogen.Task.Builder taskBuilder = com.netflix.titus.grpc.protogen.Task.newBuilder()
                .setId(coreTask.getId())
                .setJobId(coreTask.getJobId())
                .setStatus(toGrpcTaskStatus(coreTask.getStatus()))
                .addAllStatusHistory(toGrpcTaskStatusHistory(coreTask.getStatusHistory()))
                .putAllTaskContext(taskContext)
                .putAllAttributes(coreTask.getAttributes())
                .setLogLocation(toGrpcLogLocation(coreTask, logStorageInfo));

        if (coreTask instanceof ServiceJobTask) {
            ServiceJobTask serviceTask = (ServiceJobTask) coreTask;
            taskBuilder.setMigrationDetails(toGrpcMigrationDetails(serviceTask.getMigrationDetails()));
        }

        return taskBuilder.build();
    }

    public static <TASK> LogLocation toGrpcLogLocation(TASK task, LogStorageInfo<TASK> logStorageInfo) {
        LogLocation.Builder logLocationBuilder = LogLocation.newBuilder();

        // UI
        logStorageInfo.getTitusUiLink(task).ifPresent(link -> logLocationBuilder.setUi(LogLocation.UI.newBuilder().setUrl(link)));

        // Live links
        LogStorageInfo.LogLinks links = logStorageInfo.getLinks(task);
        links.getLiveLink().ifPresent(link -> logLocationBuilder.setLiveStream(LogLocation.LiveStream.newBuilder().setUrl(link)));

        // S3
        logStorageInfo.getS3LogLocation(task, true).ifPresent(s3LogLocation ->
                logLocationBuilder.setS3(
                        LogLocation.S3.newBuilder()
                                .setAccountId(s3LogLocation.getAccountId())
                                .setAccountName(s3LogLocation.getAccountName())
                                .setRegion(s3LogLocation.getRegion())
                                .setBucket(s3LogLocation.getBucket())
                                .setKey(s3LogLocation.getKey())
                ));

        return logLocationBuilder.build();
    }

    public static com.netflix.titus.grpc.protogen.MigrationDetails toGrpcMigrationDetails(MigrationDetails migrationDetails) {
        return com.netflix.titus.grpc.protogen.MigrationDetails.newBuilder()
                .setNeedsMigration(migrationDetails.isNeedsMigration())
                .setStarted(migrationDetails.getStarted())
                .setDeadline(migrationDetails.getDeadline())
                .build();
    }

    public static MigrationDetails toCoreMigrationDetails(com.netflix.titus.grpc.protogen.MigrationDetails grpcMigrationDetails) {
        return MigrationDetails.newBuilder()
                .withNeedsMigration(grpcMigrationDetails.getNeedsMigration())
                .withStarted(grpcMigrationDetails.getStarted())
                .withDeadline(grpcMigrationDetails.getDeadline())
                .build();
    }

    public static JobChangeNotification toGrpcJobChangeNotification(JobManagerEvent<?> event, GrpcObjectsCache grpcObjectsCache) {
        if (event instanceof JobUpdateEvent) {
            JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
            return JobChangeNotification.newBuilder()
                    .setJobUpdate(JobChangeNotification.JobUpdate.newBuilder()
                            .setJob(grpcObjectsCache.getJob(jobUpdateEvent.getCurrent()))
                    ).build();
        }

        TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
        return JobChangeNotification.newBuilder().setTaskUpdate(
                JobChangeNotification.TaskUpdate.newBuilder()
                        .setTask(grpcObjectsCache.getTask(taskUpdateEvent.getCurrent()))
                        .setMovedFromAnotherJob(taskUpdateEvent.isMovedFromAnotherJob())
        ).build();
    }
}
