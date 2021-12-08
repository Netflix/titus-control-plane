package com.netflix.titus.api.appscale.model;

import com.netflix.titus.api.json.ObjectMappers;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PolicyConfigurationTest {
    @Test
    public void deserializePolicyConfiguration() {
        String policyConfigStrNoMetricDimensions = "{\n" +
                "  \"name\": null,\n" +
                "  \"policyType\": \"StepScaling\",\n" +
                "  \"stepScalingPolicyConfiguration\": {\n" +
                "    \"metricAggregationType\": \"Maximum\",\n" +
                "    \"adjustmentType\": \"ChangeInCapacity\",\n" +
                "    \"minAdjustmentMagnitude\": null,\n" +
                "    \"steps\": [\n" +
                "      {\n" +
                "        \"scalingAdjustment\": 1,\n" +
                "        \"metricIntervalLowerBound\": 0.0,\n" +
                "        \"metricIntervalUpperBound\": null\n" +
                "      }\n" +
                "    ],\n" +
                "    \"coolDownSec\": 60\n" +
                "  },\n" +
                "  \"alarmConfiguration\": {\n" +
                "    \"name\": null,\n" +
                "    \"region\": null,\n" +
                "    \"actionsEnabled\": true,\n" +
                "    \"comparisonOperator\": \"GreaterThanThreshold\",\n" +
                "    \"evaluationPeriods\": 1,\n" +
                "    \"threshold\": 2.0,\n" +
                "    \"metricNamespace\": \"titus/integrationTest\",\n" +
                "    \"metricName\": \"RequestCount\",\n" +
                "    \"statistic\": \"Sum\",\n" +
                "    \"periodSec\": 60\n" +
                "  },\n" +
                "  \"targetTrackingPolicy\": null\n" +
                "}";

        PolicyConfiguration policyConfigNoMetricDimension = ObjectMappers.readValue(ObjectMappers.appScalePolicyMapper(),
                policyConfigStrNoMetricDimensions, PolicyConfiguration.class);
        assertThat(policyConfigNoMetricDimension).isNotNull();
        assertThat(policyConfigNoMetricDimension.getAlarmConfiguration().getDimensions()).isNull();

        String policyConfigStrWithMetricDimensions = "{\n" +
                "  \"name\": null,\n" +
                "  \"policyType\": \"StepScaling\",\n" +
                "  \"stepScalingPolicyConfiguration\": {\n" +
                "    \"metricAggregationType\": \"Maximum\",\n" +
                "    \"adjustmentType\": \"ChangeInCapacity\",\n" +
                "    \"minAdjustmentMagnitude\": null,\n" +
                "    \"steps\": [\n" +
                "      {\n" +
                "        \"scalingAdjustment\": 1,\n" +
                "        \"metricIntervalLowerBound\": 0.0,\n" +
                "        \"metricIntervalUpperBound\": null\n" +
                "      }\n" +
                "    ],\n" +
                "    \"coolDownSec\": 60\n" +
                "  },\n" +
                "  \"alarmConfiguration\": {\n" +
                "    \"name\": null,\n" +
                "    \"region\": null,\n" +
                "    \"actionsEnabled\": true,\n" +
                "    \"comparisonOperator\": \"GreaterThanThreshold\",\n" +
                "    \"evaluationPeriods\": 1,\n" +
                "    \"threshold\": 2.0,\n" +
                "    \"metricNamespace\": \"titus/integrationTest\",\n" +
                "    \"metricName\": \"RequestCount\",\n" +
                "    \"statistic\": \"Sum\",\n" +
                "    \"periodSec\": 60,\n" +
                "    \"unknownField\": 100,\n" +
                "    \"dimensions\": [\n" +
                "      {\n" +
                "        \"Name\": \"foo\",\n" +
                "        \"Value\": \"bar\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"Name\": \"tier\",\n" +
                "        \"Value\": \"1\"\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "  \"targetTrackingPolicy\": null\n" +
                "}";

        PolicyConfiguration policyConfigWithMetricDimensions = ObjectMappers.readValue(ObjectMappers.appScalePolicyMapper(),
                policyConfigStrWithMetricDimensions, PolicyConfiguration.class);
        assertThat(policyConfigWithMetricDimensions).isNotNull();
        assertThat(policyConfigWithMetricDimensions.getAlarmConfiguration().getDimensions()).isNotNull();
        assertThat(policyConfigWithMetricDimensions.getAlarmConfiguration().getDimensions().size()).isEqualTo(2);
    }
}
