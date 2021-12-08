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

package com.netflix.titus.master.appscale.endpoint.v3.grpc;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.appscale.model.AlarmConfiguration;
import com.netflix.titus.api.appscale.model.AutoScalingPolicy;
import com.netflix.titus.api.appscale.model.ComparisonOperator;
import com.netflix.titus.api.appscale.model.CustomizedMetricSpecification;
import com.netflix.titus.api.appscale.model.MetricAggregationType;
import com.netflix.titus.api.appscale.model.MetricDimension;
import com.netflix.titus.api.appscale.model.PolicyConfiguration;
import com.netflix.titus.api.appscale.model.PolicyType;
import com.netflix.titus.api.appscale.model.PredefinedMetricSpecification;
import com.netflix.titus.api.appscale.model.Statistic;
import com.netflix.titus.api.appscale.model.StepAdjustment;
import com.netflix.titus.api.appscale.model.StepAdjustmentType;
import com.netflix.titus.api.appscale.model.StepScalingPolicyConfiguration;
import com.netflix.titus.api.appscale.model.TargetTrackingPolicy;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicy;
import com.netflix.titus.grpc.protogen.StepAdjustments;
import com.netflix.titus.grpc.protogen.StepScalingPolicy;
import com.netflix.titus.grpc.protogen.StepScalingPolicyDescriptor;
import com.netflix.titus.grpc.protogen.TargetTrackingPolicyDescriptor;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.grpc.protogen.CustomizedMetricSpecification.StatisticOneofCase.STATISTICONEOF_NOT_SET;
import static com.netflix.titus.grpc.protogen.ScalingPolicy.ScalingPolicyDescriptorCase.STEPPOLICYDESCRIPTOR;
import static com.netflix.titus.grpc.protogen.ScalingPolicy.ScalingPolicyDescriptorCase.TARGETPOLICYDESCRIPTOR;

/**
 * Collection of functions to convert policy models from gRPC to internal formats.
 * There is no semantic validation. Conversion just attempts to set whatever gRPC
 * values are set.
 */
public class InternalModelConverters {
    private static Logger log = LoggerFactory.getLogger(InternalModelConverters.class);

    public static AutoScalingPolicy toAutoScalingPolicy(PutPolicyRequest putPolicyRequestGrpc) {
        AutoScalingPolicy.Builder autoScalingPolicyBuilder = AutoScalingPolicy.newBuilder();
        autoScalingPolicyBuilder.withJobId(putPolicyRequestGrpc.getJobId());

        if (putPolicyRequestGrpc.hasScalingPolicy()) {
            autoScalingPolicyBuilder.withPolicyConfiguration(
                    toPolicyConfiguration(putPolicyRequestGrpc.getScalingPolicy())
            );
        }

        return autoScalingPolicyBuilder.build();
    }

    public static AutoScalingPolicy toAutoScalingPolicy(UpdatePolicyRequest updatePolicyRequest) {
        AutoScalingPolicy.Builder autoScalingPolicyBuilder = AutoScalingPolicy.newBuilder();
        if (updatePolicyRequest.hasPolicyId()) {
            autoScalingPolicyBuilder.withRefId(updatePolicyRequest.getPolicyId().getId());
        }

        if (updatePolicyRequest.hasScalingPolicy()) {
            autoScalingPolicyBuilder.withPolicyConfiguration(
                    toPolicyConfiguration(updatePolicyRequest.getScalingPolicy())
            );
        }

        return autoScalingPolicyBuilder.build();
    }


    private static PolicyConfiguration toPolicyConfiguration(ScalingPolicy scalingPolicyGrpc) {
        PolicyConfiguration.Builder policyConfigBuilder = PolicyConfiguration.newBuilder();

        if (scalingPolicyGrpc.getScalingPolicyDescriptorCase() == STEPPOLICYDESCRIPTOR) {
            policyConfigBuilder.withPolicyType(PolicyType.StepScaling);

            StepScalingPolicyDescriptor stepScalingPolicyDesc = scalingPolicyGrpc.getStepPolicyDescriptor();

            // Build alarm
            if (stepScalingPolicyDesc.hasAlarmConfig()) {
                policyConfigBuilder.withAlarmConfiguration(toAlarmConfiguration(stepScalingPolicyDesc.getAlarmConfig()));
            }

            // Build step policy
            if (stepScalingPolicyDesc.hasScalingPolicy()) {
                policyConfigBuilder.withStepScalingPolicyConfiguration(toStepScalingPolicyConfiguration(stepScalingPolicyDesc.getScalingPolicy()));
            }
        } else if (scalingPolicyGrpc.getScalingPolicyDescriptorCase() == TARGETPOLICYDESCRIPTOR) {
            policyConfigBuilder.withPolicyType(PolicyType.TargetTrackingScaling);

            TargetTrackingPolicy targetTrackingPolicy = toTargetTrackingPolicy(scalingPolicyGrpc.getTargetPolicyDescriptor());
            policyConfigBuilder.withTargetTrackingPolicy(targetTrackingPolicy);
        } else {
            throw new IllegalArgumentException("Invalid ScalingPolicy Type provided " + scalingPolicyGrpc.getScalingPolicyDescriptorCase());
        }

        return policyConfigBuilder.build();
    }

    private static TargetTrackingPolicy toTargetTrackingPolicy(TargetTrackingPolicyDescriptor targetTrackingPolicyDesc) {
        TargetTrackingPolicy.Builder targetTrackingPolicyBuilder = TargetTrackingPolicy.newBuilder();

        if (targetTrackingPolicyDesc.hasTargetValue()) {
            targetTrackingPolicyBuilder.withTargetValue(targetTrackingPolicyDesc.getTargetValue().getValue());
        }
        if (targetTrackingPolicyDesc.hasScaleOutCooldownSec()) {
            targetTrackingPolicyBuilder.withScaleOutCooldownSec(targetTrackingPolicyDesc.getScaleOutCooldownSec().getValue());
        }
        if (targetTrackingPolicyDesc.hasScaleInCooldownSec()) {
            targetTrackingPolicyBuilder.withScaleInCooldownSec(targetTrackingPolicyDesc.getScaleInCooldownSec().getValue());
        }
        if (targetTrackingPolicyDesc.hasPredefinedMetricSpecification()) {
            targetTrackingPolicyBuilder.withPredefinedMetricSpecification(toPredefinedMetricSpec(targetTrackingPolicyDesc.getPredefinedMetricSpecification()));
        }
        if (targetTrackingPolicyDesc.hasDisableScaleIn()) {
            targetTrackingPolicyBuilder.withDisableScaleIn(targetTrackingPolicyDesc.getDisableScaleIn().getValue());
        }
        if (targetTrackingPolicyDesc.hasCustomizedMetricSpecification()) {
            targetTrackingPolicyBuilder.withCustomizedMetricSpecification(toCustomizedMetricSpec(targetTrackingPolicyDesc.getCustomizedMetricSpecification()));
        }

        return targetTrackingPolicyBuilder.build();
    }

    private static CustomizedMetricSpecification toCustomizedMetricSpec(com.netflix.titus.grpc.protogen.CustomizedMetricSpecification customizedMetricSpecGrpc) {
        CustomizedMetricSpecification.Builder customizedMetricSpecBuilder = CustomizedMetricSpecification.newBuilder();

        customizedMetricSpecBuilder.withMetricDimensionList(
                toMetricDimensionList(customizedMetricSpecGrpc.getDimensionsList()));
        customizedMetricSpecBuilder
                .withMetricName(customizedMetricSpecGrpc.getMetricName())
                .withNamespace(customizedMetricSpecGrpc.getNamespace());
        if (customizedMetricSpecGrpc.getStatisticOneofCase() != STATISTICONEOF_NOT_SET) {
            customizedMetricSpecBuilder.withStatistic(toStatistic(customizedMetricSpecGrpc.getStatistic()));
        }
        if (!customizedMetricSpecGrpc.getUnit().equals("")) {
            customizedMetricSpecBuilder
                    .withUnit(customizedMetricSpecGrpc.getUnit());
        }


        return customizedMetricSpecBuilder.build();
    }

    private static List<MetricDimension> toMetricDimensionList(List<com.netflix.titus.grpc.protogen.MetricDimension> metricDimensionListGrpc) {
        return metricDimensionListGrpc.stream()
                .map(InternalModelConverters::toMetricDimension)
                .collect(Collectors.toList());
    }

    private static MetricDimension toMetricDimension(com.netflix.titus.grpc.protogen.MetricDimension metricDimension) {
        return MetricDimension.newBuilder()
                .withName(metricDimension.getName())
                .withValue(metricDimension.getValue())
                .build();
    }

    private static PredefinedMetricSpecification toPredefinedMetricSpec(com.netflix.titus.grpc.protogen.PredefinedMetricSpecification predefinedMetricSpecification) {
        return PredefinedMetricSpecification.newBuilder()
                .withPredefinedMetricType(predefinedMetricSpecification.getPredefinedMetricType())
                .withResourceLabel(predefinedMetricSpecification.getResourceLabel())
                .build();
    }

    private static AlarmConfiguration toAlarmConfiguration(com.netflix.titus.grpc.protogen.AlarmConfiguration alarmConfigGrpc) {
        AlarmConfiguration.Builder alarmConfigBuilder = AlarmConfiguration.newBuilder();

        if (alarmConfigGrpc.hasActionsEnabled()) {
            alarmConfigBuilder.withActionsEnabled(alarmConfigGrpc.getActionsEnabled().getValue());
        }
        if (alarmConfigGrpc.hasEvaluationPeriods()) {
            alarmConfigBuilder.withEvaluationPeriods(alarmConfigGrpc.getEvaluationPeriods().getValue());
        }
        if (alarmConfigGrpc.hasPeriodSec()) {
            alarmConfigBuilder.withPeriodSec(alarmConfigGrpc.getPeriodSec().getValue());
        }
        if (alarmConfigGrpc.hasThreshold()) {
            alarmConfigBuilder.withThreshold(alarmConfigGrpc.getThreshold().getValue());
        }

        if (alarmConfigGrpc.getComparisonOpOneofCase() != com.netflix.titus.grpc.protogen.AlarmConfiguration.ComparisonOpOneofCase.COMPARISONOPONEOF_NOT_SET) {
            alarmConfigBuilder.withComparisonOperator(toComparisonOperator(alarmConfigGrpc.getComparisonOperator()));
        }
        if (alarmConfigGrpc.getStatisticOneofCase() != com.netflix.titus.grpc.protogen.AlarmConfiguration.StatisticOneofCase.STATISTICONEOF_NOT_SET) {
            alarmConfigBuilder.withStatistic(toStatistic(alarmConfigGrpc.getStatistic()));
        }
        alarmConfigBuilder.withDimensions(toMetricDimensionList(alarmConfigGrpc.getMetricDimensionsList()));

        // TODO(Andrew L): Do we want to just always set empty string, if unset?
        alarmConfigBuilder.withMetricNamespace(alarmConfigGrpc.getMetricNamespace());
        alarmConfigBuilder.withMetricName(alarmConfigGrpc.getMetricName());

        return alarmConfigBuilder.build();
    }

    private static StepScalingPolicyConfiguration toStepScalingPolicyConfiguration(StepScalingPolicy stepScalingPolicyGrpc) {
        StepScalingPolicyConfiguration.Builder stepScalingPolicyConfigBuilder = StepScalingPolicyConfiguration.newBuilder();

        if (stepScalingPolicyGrpc.getAdjustmentTypeOneofCase() != StepScalingPolicy.AdjustmentTypeOneofCase.ADJUSTMENTTYPEONEOF_NOT_SET) {
            stepScalingPolicyConfigBuilder.withAdjustmentType(toStepAdjustmentType(stepScalingPolicyGrpc.getAdjustmentType()));
        }
        if (stepScalingPolicyGrpc.getMetricAggOneofCase() != StepScalingPolicy.MetricAggOneofCase.METRICAGGONEOF_NOT_SET) {
            stepScalingPolicyConfigBuilder.withMetricAggregatorType(toMetricAggregationType(stepScalingPolicyGrpc.getMetricAggregationType()));
        }

        if (stepScalingPolicyGrpc.hasCooldownSec()) {
            stepScalingPolicyConfigBuilder.withCoolDownSec(stepScalingPolicyGrpc.getCooldownSec().getValue());
        }
        if (stepScalingPolicyGrpc.hasMinAdjustmentMagnitude()) {
            stepScalingPolicyConfigBuilder.withMinAdjustmentMagnitude(stepScalingPolicyGrpc.getMinAdjustmentMagnitude().getValue());
        }

        List<StepAdjustment> stepAdjustmentList = toStepAdjustmentList(stepScalingPolicyGrpc.getStepAdjustmentsList());
        stepScalingPolicyConfigBuilder.withSteps(stepAdjustmentList);

        return stepScalingPolicyConfigBuilder.build();
    }

    private static List<StepAdjustment> toStepAdjustmentList(List<StepAdjustments> stepAdjustmentsGrpcList) {
        List<StepAdjustment> stepAdjustmentList = new ArrayList<>();
        stepAdjustmentsGrpcList.forEach((stepAdjustments) -> {
            stepAdjustmentList.add(toStepAdjustment(stepAdjustments));
        });
        return stepAdjustmentList;
    }

    private static StepAdjustment toStepAdjustment(StepAdjustments stepAdjustmentsGprc) {
        StepAdjustment.Builder stepAdjustmentBuilder = StepAdjustment.newBuilder();

        if (stepAdjustmentsGprc.hasMetricIntervalLowerBound()) {
            stepAdjustmentBuilder.withMetricIntervalLowerBound(stepAdjustmentsGprc.getMetricIntervalLowerBound().getValue());
        }
        if (stepAdjustmentsGprc.hasMetricIntervalUpperBound()) {
            stepAdjustmentBuilder.withMetricIntervalUpperBound(stepAdjustmentsGprc.getMetricIntervalUpperBound().getValue());
        }
        if (stepAdjustmentsGprc.hasScalingAdjustment()) {
            stepAdjustmentBuilder.withScalingAdjustment(stepAdjustmentsGprc.getScalingAdjustment().getValue());
        }

        return stepAdjustmentBuilder.build();
    }

    private static MetricAggregationType toMetricAggregationType(StepScalingPolicy.MetricAggregationType metricAggregationTypeGrpc) {
        MetricAggregationType metricAggregationType;
        switch (metricAggregationTypeGrpc) {
            case Average:
                metricAggregationType = MetricAggregationType.Average;
                break;
            case Minimum:
                metricAggregationType = MetricAggregationType.Minimum;
                break;
            case Maximum:
                metricAggregationType = MetricAggregationType.Maximum;
                break;
            default:
                throw new IllegalArgumentException("Invalid StepScalingPolicy MetricAggregationType value " + metricAggregationTypeGrpc);
        }
        return metricAggregationType;
    }

    private static StepAdjustmentType toStepAdjustmentType(StepScalingPolicy.AdjustmentType adjustmentTypeGrpc) {
        StepAdjustmentType stepAdjustmentType;
        switch (adjustmentTypeGrpc) {
            case ExactCapacity:
                stepAdjustmentType = StepAdjustmentType.ExactCapacity;
                break;
            case ChangeInCapacity:
                stepAdjustmentType = StepAdjustmentType.ChangeInCapacity;
                break;
            case PercentChangeInCapacity:
                stepAdjustmentType = StepAdjustmentType.PercentChangeInCapacity;
                break;
            default:
                throw new IllegalArgumentException("Invalid StepScalingPolicy AdjustmentType value " + adjustmentTypeGrpc);
        }
        return stepAdjustmentType;
    }

    private static ComparisonOperator toComparisonOperator(com.netflix.titus.grpc.protogen.AlarmConfiguration.ComparisonOperator comparisonOperatorGrpc) {
        ComparisonOperator comparisonOperator;
        switch (comparisonOperatorGrpc) {
            case GreaterThanOrEqualToThreshold:
                comparisonOperator = ComparisonOperator.GreaterThanOrEqualToThreshold;
                break;
            case GreaterThanThreshold:
                comparisonOperator = ComparisonOperator.GreaterThanThreshold;
                break;
            case LessThanOrEqualToThreshold:
                comparisonOperator = ComparisonOperator.LessThanOrEqualToThreshold;
                break;
            case LessThanThreshold:
                comparisonOperator = ComparisonOperator.LessThanThreshold;
                break;
            default:
                throw new IllegalArgumentException("Invalid AlarmConfiguration ComparisonOperator value " + comparisonOperatorGrpc);
        }
        return comparisonOperator;
    }

    private static Statistic toStatistic(com.netflix.titus.grpc.protogen.AlarmConfiguration.Statistic statisticGrpc) {
        Statistic statistic;
        switch (statisticGrpc) {
            case SampleCount:
                statistic = Statistic.SampleCount;
                break;
            case Sum:
                statistic = Statistic.Sum;
                break;
            case Average:
                statistic = Statistic.Average;
                break;
            case Minimum:
                statistic = Statistic.Minimum;
                break;
            case Maximum:
                statistic = Statistic.Maximum;
                break;
            default:
                throw new IllegalArgumentException("Invalid AlarmConfiguration Statistic value " + statisticGrpc);
        }
        return statistic;
    }
}
