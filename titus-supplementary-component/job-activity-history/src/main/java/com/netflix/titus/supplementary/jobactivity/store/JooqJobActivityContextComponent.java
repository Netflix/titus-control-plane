package com.netflix.titus.supplementary.jobactivity.store;


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
public class JooqJobActivityContextComponent {

    @Bean
    public JooqConfigurationBean getJooqPropertyConfiguration() {
        return new JooqConfigurationBean();
    }

    @Bean
    public SQLDialect dialect() {
        return SQLDialect.POSTGRES;
    }

    @Bean
    public EmbeddedPostgresService getEmbeddedPostgresService(JooqConfigurationBean jooqConfiguration) {
        return new EmbeddedPostgresService(jooqConfiguration);
    }

    @Bean
    @Primary
    @Qualifier("jobActivityJooqContext")
    public JooqContext getJobActivityJooqContext(JooqConfigurationBean jooqConfiguration, EmbeddedPostgresService embeddedPostgresService) {
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
    public JooqContext getJooqProducerContext(JooqConfigurationBean jooqConfiguration, EmbeddedPostgresService embeddedPostgresService) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setAutoCommit(true);
        System.err.println("PRODUCER");

        // Connection management
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setLeakDetectionThreshold(3000);

        if (jooqConfiguration.isInMemoryDb()) {
            System.out.println("IN memory");
            hikariConfig.setDataSource(embeddedPostgresService.getDataSource());
        } else if (jooqConfiguration.isLocalDb()) {
            System.err.println("LOCAL");
            hikariConfig.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
        } else {
            System.err.println("NOT LOCAL");
            hikariConfig.addDataSourceProperty(PGProperty.SSL.getName(), "true");
            hikariConfig.addDataSourceProperty(PGProperty.SSL_MODE.getName(), "verify-ca");
            hikariConfig.addDataSourceProperty(PGProperty.SSL_FACTORY.getName(), RDSSSLSocketFactory.class.getName());
            hikariConfig.setJdbcUrl(jooqConfiguration.getProducerDatatabaseUrl());
        }
        return new JooqContext(jooqConfiguration, new HikariDataSource(hikariConfig), embeddedPostgresService);
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
