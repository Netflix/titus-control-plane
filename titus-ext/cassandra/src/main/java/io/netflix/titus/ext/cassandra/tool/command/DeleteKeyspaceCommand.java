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

package io.netflix.titus.ext.cassandra.tool.command;

import com.datastax.driver.core.Session;
import io.netflix.titus.ext.cassandra.tool.Command;
import io.netflix.titus.ext.cassandra.tool.CommandContext;
import org.apache.commons.cli.Options;

public class DeleteKeyspaceCommand implements Command {
    @Override
    public String getDescription() {
        return "Deletes keyspace";
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.TargetKeySpace;
    }

    @Override
    public Options getOptions() {
        return new Options();
    }

    @Override
    public void execute(CommandContext commandContext) {
        Session session = commandContext.getTargetSession();
        String keyspace = commandContext.getTargetKeySpace();

        if (keyspace.contains("main")) {
            throw new IllegalArgumentException("Cannot delete keyspaces that contain the word main");
        }

        boolean keyspaceExists = session.getCluster().getMetadata().getKeyspace(keyspace) != null;
        if (!keyspaceExists) {
            throw new IllegalStateException("Keyspace: " + keyspace + " does not exist.");
        }

        session.execute("DROP KEYSPACE " + keyspace);

        keyspaceExists = session.getCluster().getMetadata().getKeyspace(keyspace) != null;
        if (keyspaceExists) {
            throw new IllegalStateException("Keyspace: " + keyspace + " exists after deletion.");
        }
    }
}
