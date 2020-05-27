package com.netflix.titus.ext.jooqflyway.jobactivity;

import javax.sql.DataSource;

import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDSLContext;

public class JooqContext {

    private static final SQLDialect DEFAULT_DIALECT = SQLDialect.POSTGRES;

    private final JooqConfiguration jooqConfiguration;
    private final DataSource dataSource;
    private final EmbeddedPostgresService embeddedPostgresService;
    private final DefaultDSLContext dslContext;
    public JooqContext(JooqConfiguration jooqConfiguration,
                       DataSource dataSource,
                       EmbeddedPostgresService embeddedPostgresService) {
        this.jooqConfiguration = jooqConfiguration;
        this.dataSource = dataSource;
        this.embeddedPostgresService = embeddedPostgresService;
        this.dslContext = new DefaultDSLContext(dataSource, DEFAULT_DIALECT);
    }

    public JooqConfiguration getJooqConfiguration() {
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
