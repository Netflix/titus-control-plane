package com.netflix.titus.ext.jooqflyway;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.ext.supplementary.jobactivity")
public interface JooqConfiguration {
    @DefaultValue("jdbc://localhost")
    String getDatabaseUrl();

    @DefaultValue("false")
    boolean isInMemoryDb();

    @DefaultValue("jdbc://localhost")
    String getProducerDatatabaseUrl();

    @DefaultValue("false")
    boolean isLocalDb();
}
