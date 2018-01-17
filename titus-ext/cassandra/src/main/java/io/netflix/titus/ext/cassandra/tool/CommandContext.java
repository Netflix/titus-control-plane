/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.ext.cassandra.tool;

import com.datastax.driver.core.Session;
import io.netflix.titus.ext.cassandra.executor.AsyncCassandraExecutor;
import org.apache.commons.cli.CommandLine;

/**
 *
 */
public interface CommandContext {

    CommandLine getCommandLine();

    /**
     * Returns session not associated with any keyspace.
     */
    Session getDefaultSession();

    /**
     * Returns session associated with target (mutated) keyspace.
     */
    Session getTargetSession();

    String getTargetKeySpace();

    AsyncCassandraExecutor getTargetCassandraExecutor();

    /**
     * Returns session associated with source (read-only) keyspace.
     */
    Session getSourceSession();

    String getSourceKeySpace();

    AsyncCassandraExecutor getSourceCassandraExecutor();

    void shutdown();
}
