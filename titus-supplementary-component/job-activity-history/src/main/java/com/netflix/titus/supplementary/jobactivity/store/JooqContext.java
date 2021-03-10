package com.netflix.titus.supplementary.jobactivity.store;

import javax.sql.DataSource;

import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDSLContext;

public class JooqContext {

    private static final SQLDialect DEFAULT_DIALECT = SQLDialect.POSTGRES;

    private final JooqConfigurationBean jooqConfiguration;
    private final DataSource dataSource;
    private final EmbeddedPostgresService embeddedPostgresService;
    private final DefaultDSLContext dslContext;
    public JooqContext(JooqConfigurationBean jooqConfiguration,
                       DataSource dataSource,
                       EmbeddedPostgresService embeddedPostgresService) {
        this.jooqConfiguration = jooqConfiguration;
        this.dataSource = dataSource;
        this.embeddedPostgresService = embeddedPostgresService;
        this.dslContext = new DefaultDSLContext(dataSource, DEFAULT_DIALECT);
    }

    public JooqConfigurationBean getJooqConfiguration() {
        return jooqConfiguration;
    }

    public EmbeddedPostgresService getEmbeddedPostgresService() {
        return embeddedPostgresService;
    }

    public SQLDialect getDialect() {
        return DEFAULT_DIALECT;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public DefaultDSLContext getDslContext() {
        return dslContext;
    }
}
