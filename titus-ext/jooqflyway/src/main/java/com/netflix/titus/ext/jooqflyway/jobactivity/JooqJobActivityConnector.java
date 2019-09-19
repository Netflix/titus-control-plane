package com.netflix.titus.ext.jooqflyway.jobactivity;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;
import org.jooq.impl.DefaultDSLContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;


@Configuration
public class JooqJobActivityConnector {

    @Bean
    public JooqConfiguration getJooqConfiguration(Environment environment) {
        return new JooqConfigurationBean(environment);
    }

    @Bean
    public SQLDialect dialect() {
        return SQLDialect.POSTGRES;
    }

    @Bean
    public DataSource getDataSource(JooqConfiguration configuration) {
        HikariConfig hikariConfig = new HikariConfig();

        hikariConfig.setDriverClassName("org.postgresql.Driver");
        hikariConfig.setJdbcUrl(configuration.getJdbcUrl());
        hikariConfig.setUsername(configuration.getJdbcUsername());
        hikariConfig.setSchema(configuration.getJdbcSchema());
        // Connection management
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setRegisterMbeans(true);
        hikariConfig.setLeakDetectionThreshold(3000);
        hikariConfig.setPassword(configuration.getJdbcPassword());

        return new HikariDataSource(hikariConfig);
    }

    @Bean
    @Qualifier("jobActivityDSLContext")
    public DSLContext getJobActivityDSLContext(JooqConfiguration configuration) {
        DSLContext dslContext = DSL.using(getDataSource(configuration), dialect());
        dslContext.settings().setQueryTimeout(60);
        return dslContext;
    }

    @Bean
    @Qualifier("producerDSLContext")
    public DSLContext getProducerDSLContext(JooqConfiguration configuration) {
        ConnectionProvider connectionProvider;
        try {
            connectionProvider = new DefaultConnectionProvider(DriverManager.getConnection(configuration.getProducerDatatabaseUrl()));
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot initialize connection to Postgres database", e);
        }
        return new DefaultDSLContext(connectionProvider, SQLDialect.POSTGRES_9_5);
    }


}
