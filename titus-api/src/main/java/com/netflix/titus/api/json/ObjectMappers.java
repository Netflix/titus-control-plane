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

package com.netflix.titus.api.json;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.Deserializers;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.store.mixin.AgentInstanceGroupMixin;
import com.netflix.titus.api.agent.store.mixin.AgentInstanceMixin;
import com.netflix.titus.api.agent.store.mixin.InstanceGroupLifecycleStatusMixin;
import com.netflix.titus.api.agent.store.mixin.InstanceLifecycleStatusMixin;
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
import com.netflix.titus.api.clustermembership.connector.mixin.ClusterMemberAddressMixIn;
import com.netflix.titus.api.clustermembership.connector.mixin.ClusterMemberMixIn;
import com.netflix.titus.api.clustermembership.connector.mixin.ClusterMembershipRevisionMixIn;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Owner;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
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
import com.netflix.titus.api.jobmanager.store.mixin.AvailabilityPercentageLimitDisruptionBudgetPolicyMixIn;
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
import com.netflix.titus.api.jobmanager.store.mixin.ExponentialBackoffRetryPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.HourlyTimeWindowMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.ImageMixin;
import com.netflix.titus.api.jobmanager.store.mixin.ImmediateRetryPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.JobDescriptorExtMixin;
import com.netflix.titus.api.jobmanager.store.mixin.JobDescriptorMixin;
import com.netflix.titus.api.jobmanager.store.mixin.JobGroupInfoMixin;
import com.netflix.titus.api.jobmanager.store.mixin.JobMixin;
import com.netflix.titus.api.jobmanager.store.mixin.JobStatusMixin;
import com.netflix.titus.api.jobmanager.store.mixin.MigrationDetailsMixin;
import com.netflix.titus.api.jobmanager.store.mixin.MigrationPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.OwnerMixin;
import com.netflix.titus.api.jobmanager.store.mixin.PercentagePerHourDisruptionBudgetRateMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.RatePerIntervalDisruptionBudgetRateMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.RatePercentagePerIntervalDisruptionBudgetRateMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.RelocationLimitDisruptionBudgetPolicyMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.RetryPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.SecurityProfileMixin;
import com.netflix.titus.api.jobmanager.store.mixin.SelfManagedDisruptionBudgetPolicyMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.SelfManagedMigrationPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.ServiceJobExtMixin;
import com.netflix.titus.api.jobmanager.store.mixin.ServiceJobProcessesMixin;
import com.netflix.titus.api.jobmanager.store.mixin.ServiceJobTaskMixin;
import com.netflix.titus.api.jobmanager.store.mixin.SystemDefaultMigrationPolicyMixin;
import com.netflix.titus.api.jobmanager.store.mixin.TaskInstancesMixin;
import com.netflix.titus.api.jobmanager.store.mixin.TaskMixin;
import com.netflix.titus.api.jobmanager.store.mixin.TaskStatusMixin;
import com.netflix.titus.api.jobmanager.store.mixin.TimeWindowMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.TwoLevelResourceMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.UnhealthyTasksLimitDisruptionBudgetPolicyMixIn;
import com.netflix.titus.api.jobmanager.store.mixin.UnlimitedDisruptionBudgetRateMixIn;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.scheduler.model.Match;
import com.netflix.titus.api.scheduler.model.Must;
import com.netflix.titus.api.scheduler.model.Operator;
import com.netflix.titus.api.scheduler.model.Should;
import com.netflix.titus.api.scheduler.model.SystemSelector;
import com.netflix.titus.api.scheduler.store.mixin.MatchMixin;
import com.netflix.titus.api.scheduler.store.mixin.MustMixin;
import com.netflix.titus.api.scheduler.store.mixin.OperatorMixin;
import com.netflix.titus.api.scheduler.store.mixin.ShouldMixin;
import com.netflix.titus.api.scheduler.store.mixin.SystemSelectorMixin;
import com.netflix.titus.api.store.v2.ApplicationSlaMixIn;
import com.netflix.titus.api.store.v2.ResourceDimensionMixin;
import com.netflix.titus.common.util.PropertiesExt;
import com.netflix.titus.common.util.ReflectionExt;
import rx.exceptions.Exceptions;

/**
 * Jackon's {@link ObjectMapper} is thread safe, and uses cache for optimal performance. It makes sense
 * to reuse the same instance within single JVM. This class provides shared, pre-configured instances of
 * {@link ObjectMapper} with different configuration options.
 */
public class ObjectMappers {

    private static final ObjectMapper JACKSON_DEFAULT = new ObjectMapper();
    private static final ObjectMapper DEFAULT = createDefaultMapper();
    private static final ObjectMapper COMPACT = createCompactMapper();
    private static final ObjectMapper STORE = createStoreMapper();
    private static final ObjectMapper APP_SCALE_STORE = createAppScalePolicyMapper();
    private static final ObjectMapper PROTOBUF = createProtobufMapper();

    /**
     * A helper marker class for use with {@link JsonView} annotation.
     */
    public static final class PublicView {
    }

    public static final class DebugView {
    }

    public static ObjectMapper jacksonDefaultMapper() {
        return JACKSON_DEFAULT;
    }

    public static ObjectMapper defaultMapper() {
        return DEFAULT;
    }

    public static ObjectMapper compactMapper() {
        return COMPACT;
    }

    public static ObjectMapper storeMapper() {
        return STORE;
    }

    public static ObjectMapper appScalePolicyMapper() {
        return APP_SCALE_STORE;
    }

    public static ObjectMapper protobufMapper() {
        return PROTOBUF;
    }

    public static String writeValueAsString(ObjectMapper objectMapper, Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    public static <T> T readValue(ObjectMapper objectMapper, String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

    /**
     * Serializes only the specified fields in Titus POJOs.
     */
    public static ObjectMapper applyFieldsFilter(ObjectMapper original, Collection<String> fields) {
        Preconditions.checkArgument(!fields.isEmpty(), "Fields filter, with no field names provided");

        PropertiesExt.PropertyNode<Boolean> rootNode = PropertiesExt.fullSplit(fields);
        SimpleModule module = new SimpleModule() {
            @Override
            public void setupModule(SetupContext context) {
                super.setupModule(context);
                context.appendAnnotationIntrospector(new TitusAnnotationIntrospector());
            }
        };
        ObjectMapper newMapper = original.copy().registerModule(module);

        SimpleBeanPropertyFilter filter = new SimpleBeanPropertyFilter() {

            private PropertiesExt.PropertyNode<Boolean> findNode(JsonStreamContext outputContext) {
                if (outputContext.inArray()) {
                    return findNode(outputContext.getParent());
                }
                if (outputContext.getParent() == null) {
                    return rootNode;
                }
                PropertiesExt.PropertyNode<Boolean> node = findNode(outputContext.getParent());
                if (node == null) {
                    return null;
                }
                if (isEnabled(node)) {
                    return node;
                }
                return node.getChildren().get(outputContext.getCurrentName());
            }

            @Override
            public void serializeAsField(Object pojo, JsonGenerator jgen, SerializerProvider provider, PropertyWriter writer) throws Exception {
                PropertiesExt.PropertyNode<Boolean> selectorNode = findNode(jgen.getOutputContext().getParent());
                if (selectorNode != null) {
                    boolean enabled = isEnabled(selectorNode);
                    if (!enabled) {
                        PropertiesExt.PropertyNode<Boolean> childNode = selectorNode.getChildren().get(writer.getName());
                        if (childNode != null) {
                            boolean isNested = !childNode.getChildren().isEmpty() && !isPrimitive(writer);
                            enabled = isNested || isEnabled(childNode);
                        }
                    }
                    if (enabled) {
                        writer.serializeAsField(pojo, jgen, provider);
                        return;
                    }
                }
                if (!jgen.canOmitFields()) {
                    writer.serializeAsOmittedField(pojo, jgen, provider);
                }
            }

            private boolean isPrimitive(PropertyWriter writer) {
                if (writer instanceof BeanPropertyWriter) {
                    BeanPropertyWriter bw = (BeanPropertyWriter) writer;
                    return ReflectionExt.isPrimitiveOrWrapper(bw.getType().getRawClass());
                }
                return false;
            }

            private Boolean isEnabled(PropertiesExt.PropertyNode<Boolean> childNode) {
                return childNode.getValue().orElse(Boolean.FALSE);
            }
        };

        newMapper.setFilterProvider(new SimpleFilterProvider().addFilter("titusFilter", filter));
        return newMapper;
    }

    private static class TitusAnnotationIntrospector extends AnnotationIntrospector {
        @Override
        public Version version() {
            return Version.unknownVersion();
        }

        @Override
        public Object findFilterId(Annotated ann) {
            Object id = super.findFilterId(ann);
            if (id == null && ann.getRawType().getName().startsWith("com.netflix.titus")) {
                id = "titusFilter";
            }
            return id;
        }
    }

    private static ObjectMapper createDefaultMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
        objectMapper.configure(MapperFeature.AUTO_DETECT_FIELDS, false);

        return objectMapper;
    }

    private static ObjectMapper createCompactMapper() {
        return new ObjectMapper();
    }

    private static ObjectMapper createAppScalePolicyMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
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

        objectMapper.registerModule(new Jdk8Module());

        // Common
        objectMapper.addMixIn(ResourceDimension.class, ResourceDimensionMixin.class);

        // Capacity management
        objectMapper.addMixIn(ApplicationSLA.class, ApplicationSlaMixIn.class);

        // Agent Management
        objectMapper.addMixIn(AgentInstanceGroup.class, AgentInstanceGroupMixin.class);
        objectMapper.addMixIn(AgentInstance.class, AgentInstanceMixin.class);
        objectMapper.addMixIn(InstanceLifecycleStatus.class, InstanceLifecycleStatusMixin.class);
        objectMapper.addMixIn(InstanceGroupLifecycleStatus.class, InstanceGroupLifecycleStatusMixin.class);

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
        objectMapper.addMixIn(Image.class, ImageMixin.class);
        objectMapper.addMixIn(ServiceJobProcesses.class, ServiceJobProcessesMixin.class);

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

        // Scheduler
        objectMapper.addMixIn(Match.class, MatchMixin.class);
        objectMapper.addMixIn(Must.class, MustMixin.class);
        objectMapper.addMixIn(Operator.class, OperatorMixin.class);
        objectMapper.addMixIn(Should.class, ShouldMixin.class);
        objectMapper.addMixIn(SystemSelector.class, SystemSelectorMixin.class);

        // Cluster membership
        objectMapper.addMixIn(ClusterMemberAddress.class, ClusterMemberAddressMixIn.class);
        objectMapper.addMixIn(ClusterMember.class, ClusterMemberMixIn.class);
        objectMapper.addMixIn(ClusterMembershipRevision.class, ClusterMembershipRevisionMixIn.class);

        return objectMapper;
    }

    private static ObjectMapper createProtobufMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Serialization
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // Deserialization
        mapper.disable(SerializationFeature.INDENT_OUTPUT);

        SimpleDeserializers simpleDeserializers = new SimpleDeserializers();
        simpleDeserializers.addDeserializer(String.class, new TrimmingStringDeserializer());

        List<Deserializers> deserializersList = Arrays.asList(
                new AssignableFromDeserializers(Message.class, new ProtobufMessageDeserializer()),
                simpleDeserializers
        );
        CompositeDeserializers compositeDeserializers = new CompositeDeserializers(deserializersList);
        CustomDeserializerSimpleModule module = new CustomDeserializerSimpleModule(compositeDeserializers);
        module.addSerializer(Message.class, new ProtobufMessageSerializer());
        mapper.registerModule(module);

        return mapper;
    }

}
