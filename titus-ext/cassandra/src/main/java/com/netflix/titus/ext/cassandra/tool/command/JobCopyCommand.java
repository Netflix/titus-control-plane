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

import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.titus.ext.cassandra.tool.CassandraSchemas;
import com.netflix.titus.ext.cassandra.tool.CassandraUtils;
import com.netflix.titus.ext.cassandra.tool.Command;
import com.netflix.titus.ext.cassandra.tool.CommandContext;
import com.netflix.titus.ext.cassandra.tool.StorePreconditions;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class JobCopyCommand implements Command {

    @Override
    public String getDescription() {
        return "Copy job key space from source to target";
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.SourceTargetKeySpaces;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("a").longOpt("archive").desc("Copy archive tables").build());
        options.addOption(Option.builder("d").longOpt("clean").desc("Remove the original table content before copy").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) {
        String targetKeySpace = context.getTargetSession().getLoggedKeyspace();

        Preconditions.checkArgument(
                StorePreconditions.isDevOrBackupStack(targetKeySpace),
                "Can copy to development/backup keyspaces only, not " + targetKeySpace
        );

        boolean includeArchive = context.getCommandLine().hasOption('a');
        boolean clean = context.getCommandLine().hasOption('c');

        List<String> titusTables = includeArchive ? CassandraSchemas.JOB_TABLES : CassandraSchemas.JOB_ACTIVE_TABLES;
        if (clean) {
            titusTables.forEach(table -> CassandraUtils.truncateTable(context, table));
        }
        titusTables.forEach(table -> CassandraUtils.copyTable(context, table));
    }
}
