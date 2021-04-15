/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.ext.jooq.relocation;

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
