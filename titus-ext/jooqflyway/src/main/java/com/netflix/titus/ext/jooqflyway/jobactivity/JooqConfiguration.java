package com.netflix.titus.ext.jooqflyway.jobactivity;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.ext.jooqflyway")
public interface JooqConfiguration {

    @DefaultValue("jdbc://localhost")
    String getDatabaseUrl();

    @DefaultValue("false")
    boolean isInMemoryDb();

    @DefaultValue("jdbc:postgresql://localhost:5432/jobactivity")
    String getJdbcUrl();
}
