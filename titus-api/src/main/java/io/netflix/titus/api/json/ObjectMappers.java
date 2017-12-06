/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.json;

import java.io.IOException;
import java.util.Collection;

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
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Preconditions;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import io.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import io.netflix.titus.api.agent.model.InstanceOverrideStatus;
import io.netflix.titus.api.agent.store.mixin.AgentInstanceGroupMixin;
import io.netflix.titus.api.agent.store.mixin.AgentInstanceMixin;
import io.netflix.titus.api.agent.store.mixin.AutoScaleRuleMixin;
import io.netflix.titus.api.agent.store.mixin.InstanceGroupLifecycleStatusMixin;
import io.netflix.titus.api.agent.store.mixin.InstanceLifecycleStatusMixin;
import io.netflix.titus.api.agent.store.mixin.InstanceOverrideStatusMixin;
import io.netflix.titus.api.appscale.model.AlarmConfiguration;
import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import io.netflix.titus.api.appscale.model.CustomizedMetricSpecification;
import io.netflix.titus.api.appscale.model.MetricDimension;
import io.netflix.titus.api.appscale.model.PolicyConfiguration;
import io.netflix.titus.api.appscale.model.PredefinedMetricSpecification;
import io.netflix.titus.api.appscale.model.StepAdjustment;
import io.netflix.titus.api.appscale.model.StepScalingPolicyConfiguration;
import io.netflix.titus.api.appscale.model.TargetTrackingPolicy;
import io.netflix.titus.api.appscale.store.mixin.AlarmConfigurationMixIn;
import io.netflix.titus.api.appscale.store.mixin.AutoScalingPolicyMixIn;
import io.netflix.titus.api.appscale.store.mixin.CustomizedMetricSpecificationMixin;
import io.netflix.titus.api.appscale.store.mixin.MetricDimensionMixin;
import io.netflix.titus.api.appscale.store.mixin.PolicyConfigurationMixIn;
import io.netflix.titus.api.appscale.store.mixin.PredefinedMetricSpecificationMixin;
import io.netflix.titus.api.appscale.store.mixin.StepAdjustmentMixIn;
import io.netflix.titus.api.appscale.store.mixin.StepScalingPolicyConfigurationMixIn;
import io.netflix.titus.api.appscale.store.mixin.TargetTrackingPolicyMixin;
import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.Container;
import io.netflix.titus.api.jobmanager.model.job.ContainerResources;
import io.netflix.titus.api.jobmanager.model.job.Image;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.Owner;
import io.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.TwoLevelResource;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.model.job.migration.SystemDefaultMigrationPolicy;
import io.netflix.titus.api.jobmanager.model.job.migration.MigrationDetails;
import io.netflix.titus.api.jobmanager.model.job.migration.MigrationPolicy;
import io.netflix.titus.api.jobmanager.model.job.migration.SelfManagedMigrationPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.RetryPolicy;
import io.netflix.titus.api.jobmanager.store.mixin.BatchJobExtMixin;
import io.netflix.titus.api.jobmanager.store.mixin.BatchJobTaskMixin;
import io.netflix.titus.api.jobmanager.store.mixin.CapacityMixin;
import io.netflix.titus.api.jobmanager.store.mixin.ContainerMixin;
import io.netflix.titus.api.jobmanager.store.mixin.ContainerResourcesMixin;
import io.netflix.titus.api.jobmanager.store.mixin.ServiceJobProcessesMixin;
import io.netflix.titus.api.jobmanager.store.mixin.SystemDefaultMigrationPolicyMixin;
import io.netflix.titus.api.jobmanager.store.mixin.DelayedRetryPolicyMixin;
import io.netflix.titus.api.jobmanager.store.mixin.ImageMixin;
import io.netflix.titus.api.jobmanager.store.mixin.ImmediateRetryPolicyMixin;
import io.netflix.titus.api.jobmanager.store.mixin.JobDescriptorExtMixin;
import io.netflix.titus.api.jobmanager.store.mixin.JobDescriptorMixin;
import io.netflix.titus.api.jobmanager.store.mixin.JobGroupInfoMixin;
import io.netflix.titus.api.jobmanager.store.mixin.JobMixin;
import io.netflix.titus.api.jobmanager.store.mixin.JobStatusMixin;
import io.netflix.titus.api.jobmanager.store.mixin.MigrationDetailsMixin;
import io.netflix.titus.api.jobmanager.store.mixin.MigrationPolicyMixin;
import io.netflix.titus.api.jobmanager.store.mixin.OwnerMixin;
import io.netflix.titus.api.jobmanager.store.mixin.RetryPolicyMixin;
import io.netflix.titus.api.jobmanager.store.mixin.SecurityProfileMixin;
import io.netflix.titus.api.jobmanager.store.mixin.SelfManagedMigrationPolicyMixin;
import io.netflix.titus.api.jobmanager.store.mixin.ServiceJobExtMixin;
import io.netflix.titus.api.jobmanager.store.mixin.ServiceJobTaskMixin;
import io.netflix.titus.api.jobmanager.store.mixin.TaskInstancesMixin;
import io.netflix.titus.api.jobmanager.store.mixin.TaskMixin;
import io.netflix.titus.api.jobmanager.store.mixin.TaskStatusMixin;
import io.netflix.titus.api.jobmanager.store.mixin.TwoLevelResourceMixIn;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.store.v2.ResourceDimensionMixin;
import io.netflix.titus.common.util.PropertiesExt;
import io.netflix.titus.common.util.ReflectionExt;
import rx.exceptions.Exceptions;

/**
 * Jackon's {@link ObjectMapper} is thread safe, and uses cache for optimal performance. It makes sense
 * to reuse the same instance within single JVM. This class provides shared, pre-configured instances of
 * {@link ObjectMapper} with different configuration options.
 */
public class ObjectMappers {

    private static final ObjectMapper DEFAULT = createDefaultMapper();
    private static final ObjectMapper COMPACT = createCompactMapper();
    private static final ObjectMapper STORE = createStoreMapper();
    private static final ObjectMapper APP_SCALE_STORE = createAppScalePolicyMapper();

    /**
     * A helper marker class for use with {@link JsonView} annotation.
     */
    public static final class PublicView {
    }

    public static final class DebugView {
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
            if (id == null && ann.getRawType().getName().startsWith("io.netflix.titus")) {
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

        // Agent Management
        objectMapper.addMixIn(AgentInstanceGroup.class, AgentInstanceGroupMixin.class);
        objectMapper.addMixIn(AgentInstance.class, AgentInstanceMixin.class);
        objectMapper.addMixIn(AutoScaleRule.class, AutoScaleRuleMixin.class);
        objectMapper.addMixIn(InstanceLifecycleStatus.class, InstanceLifecycleStatusMixin.class);
        objectMapper.addMixIn(InstanceGroupLifecycleStatus.class, InstanceGroupLifecycleStatusMixin.class);
        objectMapper.addMixIn(InstanceOverrideStatus.class, InstanceOverrideStatusMixin.class);

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

        return objectMapper;
    }
}
