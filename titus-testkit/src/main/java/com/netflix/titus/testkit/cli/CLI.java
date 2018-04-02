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

package com.netflix.titus.testkit.cli;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;

import com.netflix.titus.testkit.cli.command.agent.AgentInstanceGetCommand;
import com.netflix.titus.testkit.cli.command.agent.AgentLifecycleUpdateCommand;
import com.netflix.titus.testkit.cli.command.agent.AgentObserveCommand;
import com.netflix.titus.testkit.cli.command.agent.AgentOverrideCommand;
import com.netflix.titus.testkit.cli.command.agent.AgentServerGroupGetCommand;
import com.netflix.titus.testkit.cli.command.job.JobGetCommand;
import com.netflix.titus.testkit.cli.command.job.KillTaskCommand;
import com.netflix.titus.testkit.cli.command.job.TasksGetCommand;
import com.netflix.titus.testkit.cli.command.agent.AgentInstanceGetCommand;
import com.netflix.titus.testkit.cli.command.agent.AgentLifecycleUpdateCommand;
import com.netflix.titus.testkit.cli.command.agent.AgentObserveCommand;
import com.netflix.titus.testkit.cli.command.agent.AgentOverrideCommand;
import com.netflix.titus.testkit.cli.command.agent.AgentServerGroupGetCommand;
import com.netflix.titus.testkit.cli.command.job.JobGetCommand;
import com.netflix.titus.testkit.cli.command.job.JobInServiceCommand;
import com.netflix.titus.testkit.cli.command.job.JobKillCommand;
import com.netflix.titus.testkit.cli.command.job.JobResizeCommand;
import com.netflix.titus.testkit.cli.command.job.JobSubmitCommand;
import com.netflix.titus.testkit.cli.command.job.JobTemplateCommand;
import com.netflix.titus.testkit.cli.command.job.JobsGetCommand;
import com.netflix.titus.testkit.cli.command.job.KillTaskCommand;
import com.netflix.titus.testkit.cli.command.job.ObserveJobsCommand;
import com.netflix.titus.testkit.cli.command.job.TaskGetCommand;
import com.netflix.titus.testkit.cli.command.job.TasksGetCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Titus CLI client.
 */
public class CLI {

    private static final Logger logger = LoggerFactory.getLogger(CLI.class);

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        // GRPC is using netty logger, which makes a lot of noise
        java.util.logging.Logger rootLog = java.util.logging.Logger.getLogger("");
        rootLog.setLevel(Level.WARNING);
        rootLog.getHandlers()[0].setLevel(Level.WARNING);
    }

    private static Map<String, CliCommand> COMMANDS = new TreeMap<String, CliCommand>() {{
        // Agent management
        put("agentServerGroups", new AgentServerGroupGetCommand());
        put("agentInstances", new AgentInstanceGetCommand());
        put("agentOverride", new AgentOverrideCommand());
        put("agentLifecycle", new AgentLifecycleUpdateCommand());
        put("agentObserve", new AgentObserveCommand());

        // Job management
        put("submit", new JobSubmitCommand());
        put("jobTemplate", new JobTemplateCommand());
        put("findJobs", new JobsGetCommand());
        put("findJob", new JobGetCommand());
        put("findTasks", new TasksGetCommand());
        put("findTask", new TaskGetCommand());
        put("observeJobs", new ObserveJobsCommand());
        put("inService", new JobInServiceCommand());
        put("resizeJob", new JobResizeCommand());
        put("killJob", new JobKillCommand());
        put("killTask", new KillTaskCommand());
    }};

    private final boolean helpRequested;
    private final String cmdName;
    private final String[] args;

    public CLI(String[] args) {
        this.helpRequested = hasHelpOption(args);
        if (helpRequested) {
            this.cmdName = null;
            this.args = args;
        } else {
            this.cmdName = takeCommand(args);
            this.args = Arrays.copyOfRange(args, 1, args.length);
        }
    }

    public void execute() {
        if (helpRequested) {
            printHelp();
            return;
        }

        CliCommand cmdExec = findCommand();
        CommandLine commandLine = parseOptions(cmdExec);
        CommandContext context = createCommandContext(commandLine);
        try {
            cmdExec.execute(context);
        } catch (Exception e) {
            context.shutdown();
            logger.error("Command execution failure", e);
        }
    }

    protected CommandContext createCommandContext(CommandLine commandLine) {
        return new CommandContext(commandLine);
    }

    private boolean hasHelpOption(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-h") || args[i].equals("--help")) {
                return true;
            }
        }
        return false;
    }

    private String takeCommand(String[] args) {
        if (args.length == 0) {
            printHelp();
            throw new IllegalArgumentException("Command argument expected");
        }
        return args[0];
    }

    private void printHelp() {
        PrintWriter writer = new PrintWriter(System.out);
        HelpFormatter formatter = new HelpFormatter();

        Options common = new Options();
        appendDefaultOptions(common);

        Options connectivity = new Options();
        appendConnectivityOptions(connectivity);

        writer.println("Usage: TitusCLI <cmd> -H <host> [-p <port>] [cmd options] [params]");
        writer.println();
        writer.println("Common options:");
        formatter.printOptions(writer, 128, common, 4, 4);
        writer.println();
        writer.println("Connectivity options:");
        formatter.printOptions(writer, 128, connectivity, 4, 4);
        writer.println();
        writer.println("Available commands:");
        writer.println();

        for (Map.Entry<String, CliCommand> entry : COMMANDS.entrySet()) {
            CliCommand cliCmd = entry.getValue();
            writer.println(entry.getKey() + " (" + cliCmd.getDescription() + ')');
            formatter.printOptions(writer, 128, cliCmd.getOptions(), 4, 4);
        }
        writer.flush();
    }

    private CliCommand findCommand() {
        CliCommand cmd = COMMANDS.get(cmdName);
        if (cmd == null) {
            throw new IllegalArgumentException("Unrecognized command " + cmdName);
        }
        return cmd;
    }

    private CommandLine parseOptions(CliCommand cmd) {
        Options options = cmd.getOptions();
        appendDefaultOptions(options);
        if (cmd.isRemote()) {
            appendConnectivityOptions(options);
        }
        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private void appendDefaultOptions(Options options) {
        options.addOption(Option.builder("h").longOpt("help").desc("This help information").build());
        options.addOption(Option.builder("r").longOpt("region").argName("name").hasArg()
                .desc("Region (default us-east-1)").build());
    }

    private void appendConnectivityOptions(Options options) {
        options.addOption(Option.builder("H").longOpt("host").hasArg().required().desc("Titus server host name").build());
        options.addOption(Option.builder("p").longOpt("port").hasArg().type(Number.class).desc("Titus server GRPC port").build());
    }

    public static void main(String[] args) {
        try {
            new CLI(args).execute();
        } catch (IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(-1);
        }
    }
}
