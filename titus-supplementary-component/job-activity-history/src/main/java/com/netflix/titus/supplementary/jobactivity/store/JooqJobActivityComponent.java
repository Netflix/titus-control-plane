package com.netflix.titus.supplementary.jobactivity.store;


import com.netflix.titus.common.runtime.TitusRuntime;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.postgresql.PGProperty;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JooqJobActivityComponent {
    @Bean
    public JobActivityStore getJobActivityStore(TitusRuntime titusRuntime,
                                                DSLContext jobActivityDSLContext,
                                                DSLContext producerDSLContext) {
        return new JooqJobActivityStore(titusRuntime, jobActivityDSLContext, producerDSLContext);
    }

    @Bean
    public JooqConfiguration getJooqPropertyConfiguration() {
        return new JooqConfiguration();
    }

    @Bean
    public SQLDialect dialect() {
        return SQLDialect.POSTGRES;
    }

    @Bean
    @Primary
    @Qualifier("jobActivityJooqContext")
    public JooqContext getJobActivityJooqContext(JooqConfiguration jooqConfiguration, EmbeddedPostgresService embeddedPostgresService) {
        HikariConfig hikariConfig = new HikariConfig();
        System.out.println("CONSUMER");

        hikariConfig.setAutoCommit(true);

        // Connection management
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setLeakDetectionThreshold(3000);
        if (jooqConfiguration.isInMemoryDb()) {
            hikariConfig.setDataSource(embeddedPostgresService.getDataSource());
        } else if (jooqConfiguration.isLocalDb()) {
            hikariConfig.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
        } else {
            hikariConfig.addDataSourceProperty(PGProperty.SSL.getName(), "true");
            hikariConfig.addDataSourceProperty(PGProperty.SSL_MODE.getName(), "verify-ca");
            hikariConfig.addDataSourceProperty(PGProperty.SSL_FACTORY.getName(), RDSSSLSocketFactory.class.getName());
            hikariConfig.setJdbcUrl(jooqConfiguration.getDatabaseUrl());
        }
        return new JooqContext(jooqConfiguration, new HikariDataSource(hikariConfig), embeddedPostgresService);
    }

    @Bean
    @Qualifier("producerJooqContext")
    public JooqContext getJooqProducerContext(JooqConfiguration jooqConfiguration, EmbeddedPostgresService producerEmbeddedPostgresService) {
        HikariConfig hikariConfig = new HikariConfig();
        System.out.println("PRODUCER");
        hikariConfig.setAutoCommit(true);

        // Connection management
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setLeakDetectionThreshold(3000);

        if (jooqConfiguration.isInMemoryDb()) {
            hikariConfig.setDataSource(producerEmbeddedPostgresService.getDataSource());
        } else if (jooqConfiguration.isLocalDb()) {
            hikariConfig.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
        } else {
            hikariConfig.addDataSourceProperty(PGProperty.SSL.getName(), "true");
            hikariConfig.addDataSourceProperty(PGProperty.SSL_MODE.getName(), "verify-ca");
            hikariConfig.addDataSourceProperty(PGProperty.SSL_FACTORY.getName(), RDSSSLSocketFactory.class.getName());
            hikariConfig.setJdbcUrl(jooqConfiguration.getProducerDatatabaseUrl());
        }
        return new JooqContext(jooqConfiguration, new HikariDataSource(hikariConfig), producerEmbeddedPostgresService);
    }

    @Bean
    @Primary
    @Qualifier("jobActivityDslContext")
    public DSLContext getJobActivityDSLContext(JooqContext jooqJobActivityContext) {
        return jooqJobActivityContext.getDslContext();
    }


    @Bean
    @Qualifier("producerDslContext")
    public DSLContext getProducerDSLContext(JooqContext jooqProducerContext) {
        return jooqProducerContext.getDslContext();
    }

}
