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

import org.apache.commons.cli.Options;

/**
 */
public interface Command {

    enum CommandType {
        NoKeySpace,            // A command does not interact with Cassandra
        TargetKeySpace,        // A command operates on a single Cassandra keyspace (target)
        SourceTargetKeySpaces  // A command operates on two Cassandra keyspaces (source and target)
    }

    String getDescription();

    CommandType getCommandType();

    Options getOptions();

    void execute(CommandContext commandContext) throws Exception;
}
