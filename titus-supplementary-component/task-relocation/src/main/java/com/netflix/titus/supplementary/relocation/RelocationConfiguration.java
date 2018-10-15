package com.netflix.titus.supplementary.relocation;

import com.netflix.archaius.api.annotations.DefaultValue;

public interface RelocationConfiguration {

    @DefaultValue("" + 5 * 60_000)
    long getRelocationScheduleIntervalMs();

    @DefaultValue("" + 5 * 60_000)
    long getRelocationTimeoutMs();

    @DefaultValue("" + 30_000)
    long getDataStalenessThresholdMs();
}
