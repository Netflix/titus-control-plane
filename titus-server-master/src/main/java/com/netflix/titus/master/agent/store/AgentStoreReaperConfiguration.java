package com.netflix.titus.master.agent.store;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.agent.reaper")
public interface AgentStoreReaperConfiguration {

    @DefaultValue("" + 24 * 60 * 60 * 1000)
    long getExpiredDataRetentionPeriodMs();
}
