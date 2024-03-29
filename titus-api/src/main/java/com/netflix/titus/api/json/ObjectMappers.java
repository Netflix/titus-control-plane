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

package com.netflix.titus.api.json;

import java.util.Collection;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.netflix.titus.api.appscale.model.AlarmConfiguration;
import com.netflix.titus.api.appscale.model.AutoScalingPolicy;
import com.netflix.titus.api.appscale.model.CustomizedMetricSpecification;
import com.netflix.titus.api.appscale.model.MetricDimension;
import com.netflix.titus.api.appscale.model.PolicyConfiguration;
import com.netflix.titus.api.appscale.model.PredefinedMetricSpecification;
import com.netflix.titus.api.appscale.model.StepAdjustment;
import com.netflix.titus.api.appscale.model.StepScalingPolicyConfiguration;
import com.netflix.titus.api.appscale.model.TargetTrackingPolicy;
import com.netflix.titus.api.appscale.store.mixin.AlarmConfigurationMixIn;
import com.netflix.titus.api.appscale.store.mixin.AutoScalingPolicyMixIn;
import com.netflix.titus.api.appscale.store.mixin.CustomizedMetricSpecificationMixin;
import com.netflix.titus.api.appscale.store.mixin.MetricDimensionMixin;
import com.netflix.titus.api.appscale.store.mixin.PolicyConfigurationMixIn;
import com.netflix.titus.api.appscale.store.mixin.PredefinedMetricSpecificationMixin;
import com.netflix.titus.api.appscale.store.mixin.StepAdjustmentMixIn;
import com.netflix.titus.api.appscale.store.mixin.StepScalingPolicyConfigurationMixIn;
import com.netflix.titus.api.appscale.store.mixin.TargetTrackingPolicyMixin;
import com.netflix.titus.api.jobmanager.model.job.BasicContainer;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.NetworkConfiguration;
import com.netflix.titus.api.jobmanager.model.job.Owner;
import com.netflix.titus.api.jobmanager.model.job.PlatformSidecar;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import com.netflix.titus.api.jobmanager.model.job.Version;
import com.netflix.titus.api.jobmanager.model.job.VolumeMount;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.AvailabilityPercentageLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.ContainerHealthProvider;
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
import com.netflix.titus.api.jobmanager.model.job.volume.SaaSVolumeSource;
import com.netflix.titus.api.jobmanager.model.job.volume.SharedContainerVolumeSource;
import com.netflix.titus.api.jobmanager.model.job.volume.Volume;
import com.netflix.titus.api.jobmanager.model.job.volume.VolumeSource;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressAllocation;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressLocation;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.jobmanager.store.mixin.AvailabilityPercentageLimitDisruptionBudgetPolicyMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.BasicContainerMixin;
import com.netflix.titus.api.jobmanager.store.mixin.BatchJobExtMixin;
import com.netflix.titus.api.jobmanager.store.mixin.BatchJobTaskMixin;
import com.netflix.titus.api.jobmanager.store.mixin.CapacityMixin;
import com.netflix.titus.api.jobmanager.store.mixin.ContainerHealthProviderMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.ContainerMixin;
import com.netflix.titus.api.jobmanager.store.mixin.ContainerResourcesMixin;
import com.netflix.titus.api.jobmanager.store.mixin.DelayedRetryPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.DisruptionBudgetMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.DisruptionBudgetPolicyMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.DisruptionBudgetRateMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.EbsVolumeMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.ExponentialBackoffRetryPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.HourlyTimeWindowMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.ImageMixin;
import com.netflix.titus.api.jobmanager.store.mixin.ImmediateRetryPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.IpAddressAllocationMixin;
import com.netflix.titus.api.jobmanager.store.mixin.IpAddressLocationMixin;
import com.netflix.titus.api.jobmanager.store.mixin.JobDescriptorExtMixin;
import com.netflix.titus.api.jobmanager.store.mixin.JobDescriptorMixin;
import com.netflix.titus.api.jobmanager.store.mixin.JobGroupInfoMixin;
import com.netflix.titus.api.jobmanager.store.mixin.JobMixin;
import com.netflix.titus.api.jobmanager.store.mixin.JobStatusMixin;
import com.netflix.titus.api.jobmanager.store.mixin.MigrationDetailsMixin;
import com.netflix.titus.api.jobmanager.store.mixin.MigrationPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.NetworkConfigurationMixin;
import com.netflix.titus.api.jobmanager.store.mixin.OwnerMixin;
import com.netflix.titus.api.jobmanager.store.mixin.PercentagePerHourDisruptionBudgetRateMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.PlatformSidecarMixin;
import com.netflix.titus.api.jobmanager.store.mixin.RatePerIntervalDisruptionBudgetRateMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.RatePercentagePerIntervalDisruptionBudgetRateMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.RelocationLimitDisruptionBudgetPolicyMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.RetryPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.SaaSVolumeSourceMixin;
import com.netflix.titus.api.jobmanager.store.mixin.SecurityProfileMixin;
import com.netflix.titus.api.jobmanager.store.mixin.SelfManagedDisruptionBudgetPolicyMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.SelfManagedMigrationPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.ServiceJobExtMixin;
import com.netflix.titus.api.jobmanager.store.mixin.ServiceJobProcessesMixin;
import com.netflix.titus.api.jobmanager.store.mixin.ServiceJobTaskMixin;
import com.netflix.titus.api.jobmanager.store.mixin.SharedContainerVolumeSourceMixin;
import com.netflix.titus.api.jobmanager.store.mixin.SignedIpAddressAllocationMixin;
import com.netflix.titus.api.jobmanager.store.mixin.SystemDefaultMigrationPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.TaskInstancesMixin;
import com.netflix.titus.api.jobmanager.store.mixin.TaskMixin;
import com.netflix.titus.api.jobmanager.store.mixin.TaskStatusMixin;
import com.netflix.titus.api.jobmanager.store.mixin.TimeWindowMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.TwoLevelResourceMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.UnhealthyTasksLimitDisruptionBudgetPolicyMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.UnlimitedDisruptionBudgetRateMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.VersionMixin;
import com.netflix.titus.api.jobmanager.store.mixin.VolumeMixin;
import com.netflix.titus.api.jobmanager.store.mixin.VolumeMountMixin;
import com.netflix.titus.api.jobmanager.store.mixin.VolumeSourceMixin;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.store.v2.ApplicationSlaMixIn;
import com.netflix.titus.api.store.v2.ResourceDimensionMixin;
import com.netflix.titus.common.util.jackson.CommonObjectMappers;

/**
 * Jackson's {@link ObjectMapper} is thread safe, and uses cache for optimal performance. It makes sense
 * to reuse the same instance within single JVM. This class provides shared, pre-configured instances of
 * {@link ObjectMapper} with different configuration options.
 */
public class ObjectMappers {

    private static final ObjectMapper STORE = createStoreMapper();
    private static final ObjectMapper APP_SCALE_STORE = createAppScalePolicyMapper();

    public static ObjectMapper storeMapper() {
        return STORE;
    }

    public static ObjectMapper appScalePolicyMapper() {
        return APP_SCALE_STORE;
    }

    @Deprecated
    public static ObjectMapper jacksonDefaultMapper() {
        return CommonObjectMappers.jacksonDefaultMapper();
    }

    @Deprecated
    public static ObjectMapper defaultMapper() {
        return CommonObjectMappers.defaultMapper();
    }

    @Deprecated
    public static ObjectMapper compactMapper() {
        return CommonObjectMappers.compactMapper();
    }

    @Deprecated
    public static ObjectMapper protobufMapper() {
        return CommonObjectMappers.protobufMapper();
    }

    @Deprecated
    public static String writeValueAsString(ObjectMapper objectMapper, Object object) {
        return CommonObjectMappers.writeValueAsString(objectMapper, object);
    }

    @Deprecated
    public static <T> T readValue(ObjectMapper objectMapper, String json, Class<T> clazz) {
        return CommonObjectMappers.readValue(objectMapper, json, clazz);
    }

    @Deprecated
    public static ObjectMapper applyFieldsFilter(ObjectMapper original, Collection<String> fields) {
        return CommonObjectMappers.applyFieldsFilter(original, fields);
    }

    private static ObjectMapper createAppScalePolicyMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.addMixIn(AlarmConfiguration.class, AlarmConfigurationMixIn.class);
        objectMapper.addMixIn(StepAdjustment.class, StepAdjustmentMixIn.class);
        objectMapper.addMixIn(StepScalingPolicyConfiguration.class, StepScalingPolicyConfigurationMixIn.class);
        objectMapper.addMixIn(PolicyConfiguration.class, PolicyConfigurationMixIn.class);
        objectMapper.addMixIn(AutoScalingPolicy.class, AutoScalingPolicyMixIn.class);
        objectMapper.addMixIn(TargetTrackingPolicy.class, TargetTrackingPolicyMixin.class);
        objectMapper.addMixIn(PredefinedMetricSpecification.class, PredefinedMetricSpecificationMixin.class);
        objectMapper.addMixIn(CustomizedMetricSpecification.class, CustomizedMetricSpecificationMixin.class);
        objectMapper.addMixIn(MetricDimension.class, MetricDimensionMixin.class);

        return objectMapper;
    }

    private static ObjectMapper createStoreMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.registerModule(new Jdk8Module());

        // Common
        objectMapper.addMixIn(ResourceDimension.class, ResourceDimensionMixin.class);

        // Capacity management
        objectMapper.addMixIn(ApplicationSLA.class, ApplicationSlaMixIn.class);

        // Job Management
        objectMapper.addMixIn(Capacity.class, CapacityMixin.class);
        objectMapper.addMixIn(Job.class, JobMixin.class);
        objectMapper.addMixIn(JobDescriptor.class, JobDescriptorMixin.class);
        objectMapper.addMixIn(JobDescriptor.JobDescriptorExt.class, JobDescriptorExtMixin.class);
        objectMapper.addMixIn(JobStatus.class, JobStatusMixin.class);
        objectMapper.addMixIn(JobGroupInfo.class, JobGroupInfoMixin.class);
        objectMapper.addMixIn(Owner.class, OwnerMixin.class);
        objectMapper.addMixIn(Capacity.class, TaskInstancesMixin.class);
        objectMapper.addMixIn(RetryPolicy.class, RetryPolicyMixin.class);
        objectMapper.addMixIn(ImmediateRetryPolicy.class, ImmediateRetryPolicyMixin.class);
        objectMapper.addMixIn(DelayedRetryPolicy.class, DelayedRetryPolicyMixin.class);
        objectMapper.addMixIn(ExponentialBackoffRetryPolicy.class, ExponentialBackoffRetryPolicyMixin.class);
        objectMapper.addMixIn(MigrationPolicy.class, MigrationPolicyMixin.class);
        objectMapper.addMixIn(SystemDefaultMigrationPolicy.class, SystemDefaultMigrationPolicyMixin.class);
        objectMapper.addMixIn(SelfManagedMigrationPolicy.class, SelfManagedMigrationPolicyMixin.class);
        objectMapper.addMixIn(MigrationDetails.class, MigrationDetailsMixin.class);
        objectMapper.addMixIn(Task.class, TaskMixin.class);
        objectMapper.addMixIn(TwoLevelResource.class, TwoLevelResourceMixIn.class);
        objectMapper.addMixIn(BatchJobTask.class, BatchJobTaskMixin.class);
        objectMapper.addMixIn(ServiceJobTask.class, ServiceJobTaskMixin.class);
        objectMapper.addMixIn(BatchJobExt.class, BatchJobExtMixin.class);
        objectMapper.addMixIn(ServiceJobExt.class, ServiceJobExtMixin.class);
        objectMapper.addMixIn(TaskStatus.class, TaskStatusMixin.class);
        objectMapper.addMixIn(ContainerResources.class, ContainerResourcesMixin.class);
        objectMapper.addMixIn(SecurityProfile.class, SecurityProfileMixin.class);
        objectMapper.addMixIn(Container.class, ContainerMixin.class);
        objectMapper.addMixIn(BasicContainer.class, BasicContainerMixin.class);
        objectMapper.addMixIn(Image.class, ImageMixin.class);
        objectMapper.addMixIn(ServiceJobProcesses.class, ServiceJobProcessesMixin.class);
        objectMapper.addMixIn(NetworkConfiguration.class, NetworkConfigurationMixin.class);
        objectMapper.addMixIn(Version.class, VersionMixin.class);
        objectMapper.addMixIn(Volume.class, VolumeMixin.class);
        objectMapper.addMixIn(VolumeMount.class, VolumeMountMixin.class);
        objectMapper.addMixIn(VolumeSource.class, VolumeSourceMixin.class);
        objectMapper.addMixIn(SharedContainerVolumeSource.class, SharedContainerVolumeSourceMixin.class);
        objectMapper.addMixIn(SaaSVolumeSource.class, SaaSVolumeSourceMixin.class);
        objectMapper.addMixIn(PlatformSidecar.class, PlatformSidecarMixin.class);

        objectMapper.addMixIn(IpAddressLocation.class, IpAddressLocationMixin.class);
        objectMapper.addMixIn(IpAddressAllocation.class, IpAddressAllocationMixin.class);
        objectMapper.addMixIn(SignedIpAddressAllocation.class, SignedIpAddressAllocationMixin.class);
        objectMapper.addMixIn(EbsVolume.class, EbsVolumeMixIn.class);

        objectMapper.addMixIn(DisruptionBudget.class, DisruptionBudgetMixIn.class);

        objectMapper.addMixIn(DisruptionBudgetPolicy.class, DisruptionBudgetPolicyMixIn.class);
        objectMapper.addMixIn(AvailabilityPercentageLimitDisruptionBudgetPolicy.class, AvailabilityPercentageLimitDisruptionBudgetPolicyMixIn.class);
        objectMapper.addMixIn(SelfManagedDisruptionBudgetPolicy.class, SelfManagedDisruptionBudgetPolicyMixIn.class);
        objectMapper.addMixIn(UnhealthyTasksLimitDisruptionBudgetPolicy.class, UnhealthyTasksLimitDisruptionBudgetPolicyMixIn.class);
        objectMapper.addMixIn(RelocationLimitDisruptionBudgetPolicy.class, RelocationLimitDisruptionBudgetPolicyMixIn.class);

        objectMapper.addMixIn(DisruptionBudgetRate.class, DisruptionBudgetRateMixIn.class);
        objectMapper.addMixIn(PercentagePerHourDisruptionBudgetRate.class, PercentagePerHourDisruptionBudgetRateMixIn.class);
        objectMapper.addMixIn(RatePerIntervalDisruptionBudgetRate.class, RatePerIntervalDisruptionBudgetRateMixIn.class);
        objectMapper.addMixIn(RatePercentagePerIntervalDisruptionBudgetRate.class, RatePercentagePerIntervalDisruptionBudgetRateMixIn.class);
        objectMapper.addMixIn(UnlimitedDisruptionBudgetRate.class, UnlimitedDisruptionBudgetRateMixIn.class);

        objectMapper.addMixIn(ContainerHealthProvider.class, ContainerHealthProviderMixIn.class);

        objectMapper.addMixIn(TimeWindow.class, TimeWindowMixIn.class);
        objectMapper.addMixIn(HourlyTimeWindow.class, HourlyTimeWindowMixIn.class);

        return objectMapper;
    }
}
