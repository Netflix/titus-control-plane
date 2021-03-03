package com.netflix.titus.ext.jooqflyway.jobactivity;

import java.sql.Connection;
import java.sql.DriverManager;

import com.netflix.titus.common.util.ExceptionExt;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConnectionProvider;
import org.jooq.impl.DefaultDSLContext;
import org.junit.rules.ExternalResource;

public class JooqResource extends ExternalResource {

    private Connection connection;
    private DefaultDSLContext dslContext;

    @Override
    protected void before() throws Throwable {
        this.connection = DriverManager.getConnection("jdbc:hsqldb:mem:junit" + System.currentTimeMillis(), "SA", "");
        this.dslContext = new DefaultDSLContext(new DefaultConnectionProvider(connection), SQLDialect.HSQLDB);
    }

    @Override
    protected void after() {
        if (dslContext != null) {
            dslContext.execute("SHUTDOWN");
            dslContext.close();
        }
        if (connection != null) {
            ExceptionExt.silent(() -> connection.close());
        }
    }

    public DefaultDSLContext getDslContext() {
        return dslContext;
    }
}
