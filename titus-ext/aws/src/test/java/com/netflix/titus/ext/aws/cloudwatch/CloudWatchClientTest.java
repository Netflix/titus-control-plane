package com.netflix.titus.ext.aws.cloudwatch;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.netflix.titus.api.appscale.model.AlarmConfiguration;
import com.netflix.titus.api.appscale.model.ComparisonOperator;
import com.netflix.titus.api.appscale.model.MetricDimension;
import com.netflix.titus.api.appscale.model.Statistic;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class CloudWatchClientTest {

    private AlarmConfiguration.Builder getAlarmConfigBuilder() {
        return AlarmConfiguration.newBuilder()
                .withName("alarm-config-1")
                .withMetricName("metric-1")
                .withMetricNamespace("standard")
                .withStatistic(Statistic.Average)
                .withActionsEnabled(true)
                .withPeriodSec(60)
                .withThreshold(5)
                .withEvaluationPeriods(2)
                .withComparisonOperator(ComparisonOperator.GreaterThanOrEqualToThreshold);
    }

    @Test
    public void buildDefaultMetricDimensions() {
        AlarmConfiguration alarmConfiguration = getAlarmConfigBuilder().build();
        List<Dimension> dimensions = CloudWatchClient.buildMetricDimensions(alarmConfiguration, "foo-bar");
        assertThat(dimensions).isNotNull();
        assertThat(dimensions.size()).isEqualTo(1);
        assertThat(dimensions.get(0).getName()).isEqualTo("AutoScalingGroupName");
        assertThat(dimensions.get(0).getValue()).isEqualTo("foo-bar");
    }

    @Test
    public void buildCustomMetricDimensions() {
        MetricDimension md1 = MetricDimension.newBuilder().withName("foo").withValue("bar").build();
        MetricDimension md2 = MetricDimension.newBuilder().withName("service-tier").withValue("1").build();
        List<MetricDimension> customMetricDimensions = Arrays.asList(md1, md2);
        AlarmConfiguration alarmConfiguration = getAlarmConfigBuilder().withDimensions(customMetricDimensions).build();
        List<Dimension> dimensions = CloudWatchClient.buildMetricDimensions(alarmConfiguration, "foo-bar");
        assertThat(dimensions).isNotNull();
        assertThat(dimensions.size()).isEqualTo(2);
        assertThat(dimensions.get(0).getName()).isEqualTo("foo");
        assertThat(dimensions.get(0).getValue()).isEqualTo("bar");
        assertThat(dimensions.get(1).getName()).isEqualTo("service-tier");
        assertThat(dimensions.get(1).getValue()).isEqualTo("1");
    }

}