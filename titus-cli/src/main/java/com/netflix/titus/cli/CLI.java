/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.cli;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.netflix.titus.cli.command.HealthCommand;
import com.netflix.titus.cli.command.eviction.EvictionEventsCommand;
import com.netflix.titus.cli.command.job.JobGetCommand;
import com.netflix.titus.cli.command.job.JobInServiceCommand;
import com.netflix.titus.cli.command.job.JobKillCommand;
import com.netflix.titus.cli.command.job.JobProcessesCommand;
import com.netflix.titus.cli.command.job.JobResizeCommand;
import com.netflix.titus.cli.command.job.JobSubmitCommand;
import com.netflix.titus.cli.command.job.JobTemplateCommand;
import com.netflix.titus.cli.command.job.JobsGetCommand;
import com.netflix.titus.cli.command.job.KillTaskCommand;
import com.netflix.titus.cli.command.job.ObserveJobsCommand;
import com.netflix.titus.cli.command.job.TaskGetCommand;
import com.netflix.titus.cli.command.job.TasksGetCommand;
import com.netflix.titus.cli.command.job.unschedulable.PrintUnschedulableJobsCommand;
import com.netflix.titus.cli.command.job.unschedulable.RemoveUnschedulableJobsCommand;
import com.netflix.titus.cli.command.scheduler.ObserveSchedulingResultCommand;
import com.netflix.titus.cli.command.supervisor.SupervisorObserveEventsCommand;
import io.grpc.StatusRuntimeException;
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

    private static final Map<String, CliCommand> BUILTIN_COMMANDS = ImmutableMap.<String, CliCommand>builder()
            .put("health", new HealthCommand())
            // Job management
            .put("submit", new JobSubmitCommand())
            .put("jobTemplate", new JobTemplateCommand())
            .put("findJobs", new JobsGetCommand())
            .put("findJob", new JobGetCommand())
            .put("findTasks", new TasksGetCommand())
            .put("findTask", new TaskGetCommand())
            .put("observeJobs", new ObserveJobsCommand())
            .put("inService", new JobInServiceCommand())
            .put("jobProcesses", new JobProcessesCommand())
            .put("resizeJob", new JobResizeCommand())
            .put("killJob", new JobKillCommand())
            .put("killTask", new KillTaskCommand())
            // Eviction
            .put("evictionEvents", new EvictionEventsCommand())
            // Scheduler
            .put("schedulingResults", new ObserveSchedulingResultCommand())
            // Supervisor
            .put("supervisorEvents", new SupervisorObserveEventsCommand())
            // Unschedulable jobs
            .put("printUnschedulableJobs", new PrintUnschedulableJobsCommand())
            .put("removeUnschedulableJobs", new RemoveUnschedulableJobsCommand())
            .build();

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
            if (e instanceof StatusRuntimeException) {
                GrpcClientErrorUtils.printDetails((StatusRuntimeException) e);
            }
        }
    }

    protected CommandContext createCommandContext(CommandLine commandLine) {
        return new CommandContext(commandLine);
    }

    protected Map<String, CliCommand> getCommandOverrides() {
        return Collections.emptyMap();
    }

    private Map<String, CliCommand> getCommands() {
        Map<String, CliCommand> merged = new HashMap<>(BUILTIN_COMMANDS);
        merged.putAll(getCommandOverrides());
        return ImmutableSortedMap.<String, CliCommand>naturalOrder().putAll(merged).build();
    }

    private boolean hasHelpOption(String[] args) {
        for (String arg : args) {
            if (arg.equals("-h") || arg.equals("--help")) {
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

        for (Map.Entry<String, CliCommand> entry : getCommands().entrySet()) {
            CliCommand cliCmd = entry.getValue();
            writer.println(entry.getKey() + " (" + cliCmd.getDescription() + ')');
            formatter.printOptions(writer, 128, cliCmd.getOptions(), 4, 4);
        }
        writer.flush();
    }

    private CliCommand findCommand() {
        CliCommand cmd = getCommands().get(cmdName);
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
