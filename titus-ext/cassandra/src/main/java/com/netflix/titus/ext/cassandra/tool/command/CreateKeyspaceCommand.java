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

package com.netflix.titus.ext.cassandra.tool.command;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.netflix.titus.ext.cassandra.tool.Command;
import com.netflix.titus.ext.cassandra.tool.CommandContext;
import com.netflix.titus.common.util.IOExt;
import com.netflix.titus.common.util.StringExt;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateKeyspaceCommand implements Command {
    private static final Logger logger = LoggerFactory.getLogger(CreateKeyspaceCommand.class);
    private static final String TABLES_CQL_FILE = "tables.cql";

    @Override
    public String getDescription() {
        return "Creates keyspace if it does not already exist and all the tables" +
                "under that keyspace";
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.NoKeySpace;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("k")
                .longOpt("keyspace")
                .desc("The name of the keyspace")
                .hasArg()
                .required()
                .build());
        options.addOption(Option.builder("r")
                .longOpt("replication")
                .desc("The replication configuration to use for keyspace creation")
                .hasArg()
                .required()
                .build());
        return options;
    }

    @Override
    public void execute(CommandContext commandContext) throws Exception {
        CommandLine commandLine = commandContext.getCommandLine();
        String keyspaceOption = commandLine.getOptionValue("keyspace");
        Set<String> keyspaces = new HashSet<>(StringExt.splitByComma(keyspaceOption));
        Session session = commandContext.getDefaultSession();

        Preconditions.checkArgument(!keyspaces.isEmpty(), "You must specify at least one keyspace.");

        logger.info("Verifying that keyspaces: {} do not exist.", keyspaces);
        for (String keyspace : keyspaces) {
            boolean keyspaceExists = session.getCluster().getMetadata().getKeyspace(keyspace) != null;
            if (keyspaceExists) {
                throw new IllegalStateException("Keyspace: " + keyspace + " already exists. You must first delete it.");
            }
        }

        String replication = commandLine.getOptionValue("replication");
        List<String> cqlStatements = convertFileToCqlQueries();

        for (String keyspace : keyspaces) {
            logger.info("Creating keyspace: {}", keyspace);

            session.execute("CREATE KEYSPACE " + keyspace + " " + replication);
            session.execute("USE " + keyspace);

            for (String cqlStatement : cqlStatements) {
                session.execute(cqlStatement);
            }
        }
    }

    private List<String> convertFileToCqlQueries() throws IOException {
        InputStream fileResourceAsStream = IOExt.getFileResourceAsStream(TABLES_CQL_FILE);
        Preconditions.checkNotNull(fileResourceAsStream, TABLES_CQL_FILE + " was not found");
        List<String> lines = IOExt.readLines(new InputStreamReader(fileResourceAsStream));
        List<String> cqlQueries = new ArrayList<>();
        StringBuilder cqlQuery = new StringBuilder();

        for (String line : lines) {
            if (Strings.isNullOrEmpty(line)) {
                continue;
            }

            boolean queryEndLine = line.contains(";");
            cqlQuery.append(line);

            if (queryEndLine) {
                cqlQueries.add(cqlQuery.toString());
                cqlQuery = new StringBuilder();
            } else {
                cqlQuery.append("\n");
            }
        }

        return cqlQueries;
    }
}
