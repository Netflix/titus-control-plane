package com.netflix.titus.ext.jooqflyway.jobactivity.producer;

import javax.sql.DataSource;

import com.netflix.titus.ext.jooqflyway.jobactivity.RDSSSLSocketFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.postgresql.PGProperty;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class JooqJobActivityProducerConnectorComponent {
    @Bean
    public JooqProducerConfiguration getJooqConfiguration(Environment environment) {
        return new JooqProducerConfiguration(environment);
    }

    @Bean
    public SQLDialect dialect() {
        return SQLDialect.POSTGRES;
    }

    @Bean
    public DataSource getProducerDataSource(JooqProducerConfiguration jooqProducerConfiguration) {
        HikariConfig hikariConfig = new HikariConfig();

        hikariConfig.setDriverClassName("org.postgresql.Driver");
        hikariConfig.addDataSourceProperty(PGProperty.SSL.getName(), "true");
        hikariConfig.addDataSourceProperty(PGProperty.SSL_MODE.getName(), "verify-ca");
        hikariConfig.addDataSourceProperty(PGProperty.SSL_FACTORY.getName(), RDSSSLSocketFactory.class.getName());
        hikariConfig.setJdbcUrl(jooqProducerConfiguration.getDatabaseUrl());

        // Connection management
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setLeakDetectionThreshold(3000);

        return new HikariDataSource(hikariConfig);
    }

    @Bean
    @Qualifier("producerDSLContext")
    public DSLContext getProducerDSLContext(JooqProducerConfiguration configuration) {
        DSLContext dslContext = DSL.using(getProducerDataSource(configuration), dialect());
        dslContext.settings().setQueryTimeout(60);
        return dslContext;
    }
}
