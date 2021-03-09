package com.netflix.titus.supplementary.jobactivity;

import com.netflix.archaius.api.annotations.DefaultValue;
import org.springframework.context.annotation.Configuration;

@Configuration("titus.jobactivity")
public interface JobActivityConfiguration {
    /**
     * Interval at which the consumer drains the producer queue
     */
    @DefaultValue("30000")
    long getConsumerIntervalMs();

    @DefaultValue("30000")
    long getDataStalenessThresholdMs();
}
