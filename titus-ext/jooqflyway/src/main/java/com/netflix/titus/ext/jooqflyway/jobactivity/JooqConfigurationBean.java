package com.netflix.titus.ext.jooqflyway.jobactivity;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.util.SpringConfigurationUtil;
import org.springframework.core.env.Environment;

@Singleton
public class JooqConfigurationBean implements JooqConfiguration {
    private static final String PREFIX = "titus.ext.jooqflyway.";

    private final Environment environment;

    @Inject
    public JooqConfigurationBean(Environment environment) {
        this.environment = environment;
    }

    @Override
    public String getDatabaseUrl() {
        return SpringConfigurationUtil.getString(environment, PREFIX + "databaseUrl", "jdbc://notSet");
    }

    @Override
    public boolean isInMemoryDb() {
        return SpringConfigurationUtil.getBoolean(environment, PREFIX + "inMemoryDb", false);
    }

    @Override
    public String getJdbcUrl() {
        return SpringConfigurationUtil.getString(environment, PREFIX + "jdcbUrl", "jdbc:postgresql://localhost:5432/jobactivity");
    }

    @Override
    public String getJdbcSchema() {
        return SpringConfigurationUtil.getString(environment, PREFIX + "jdbcSchema", "public");
    }

    @Override
    public String getJdbcUsername() {
        return SpringConfigurationUtil.getString(environment, PREFIX + "jdbcUsername", "activityuser");
    }

    @Override
    public String getJdbcPassword() {
        return SpringConfigurationUtil.getString(environment, PREFIX + "jdbcPassword", "activitypassword");
    }

    @Override
    public String getProducerDatatabaseUrl() {
        return SpringConfigurationUtil.getString(environment, PREFIX + "databaseUrl", "jdbc://notSet");
    }
}

