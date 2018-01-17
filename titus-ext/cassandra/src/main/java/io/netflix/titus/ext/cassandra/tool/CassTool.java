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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import io.netflix.titus.ext.cassandra.tool.command.CreateKeyspaceCommand;
import io.netflix.titus.ext.cassandra.tool.command.DeleteKeyspaceCommand;
import io.netflix.titus.ext.cassandra.tool.command.JobCopyCommand;
import io.netflix.titus.ext.cassandra.tool.command.TestStoreLoadCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassTool extends AbstractCassTool {

    private static final Logger logger = LoggerFactory.getLogger(CassTool.class);

    public static final Map<String, Command> COMMANDS = ImmutableMap.<String, Command>builder()
            .put("jobCopy", new JobCopyCommand())
            .put("createKeyspace", new CreateKeyspaceCommand())
            .put("deleteKeyspace", new DeleteKeyspaceCommand())
            .put("testStoreLoad", new TestStoreLoadCommand())
            .build();

    public CassTool(String[] args) {
        super(args, COMMANDS);
    }

    @Override
    protected Options buildOptions(Command command) {
        Options options = super.buildOptions(command);
        options.addOption(Option.builder("H")
                .longOpt("target")
                .desc("The cassandra target host")
                .hasArg()
                .required()
                .build());
        options.addOption(Option.builder("p")
                .longOpt("port")
                .desc("The cassandra port")
                .hasArg()
                .required()
                .build());
        return options;
    }

    @Override
    protected CommandContext newContext(CommandLine cli) {
        return DefaultCommandContext.newCommandContext(cli);
    }

    public static void main(String[] args) {
        boolean execOK = false;
        try {
            execOK = new CassTool(args).execute();
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage(), e);
            System.exit(-1);
        }
        System.exit(execOK ? 0 : -2);
    }
}
