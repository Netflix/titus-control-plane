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

package com.netflix.titus.ext.cassandra.tool;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCassTool {

    private static final Logger logger = LoggerFactory.getLogger(AbstractCassTool.class);

    private final String cmdName;
    private final boolean helpRequested;
    private final CommandContext context;
    private final Map<String, Command> commands;

    protected AbstractCassTool(String[] args, Map<String, Command> commands) {
        this.commands = commands;
        this.helpRequested = args.length == 0 || hasHelpOption(args);
        if (helpRequested) {
            this.context = null;
            this.cmdName = null;
        } else {
            this.cmdName = args[0];
            Command command = findCommand();
            String[] cmdOptions = Arrays.copyOfRange(args, 1, args.length);
            CommandLine cli = parseOptions(cmdOptions, buildOptions(command));
            this.context = newContext(cli);
        }
    }

    protected abstract CommandContext newContext(CommandLine cli);

    public boolean execute() {
        if (helpRequested) {
            printHelp();
            return true;
        }

        Command cmdExec = findCommand();

        logger.info("Executing command {}...", cmdName);
        long startTime = System.currentTimeMillis();
        try {
            cmdExec.execute(context);
        } catch (Exception e) {
            logger.error("Command execution failure", e);
            return false;
        } finally {
            context.shutdown();
        }

        logger.info("Command {} executed in {}[ms]", cmdName, System.currentTimeMillis() - startTime);
        return true;
    }

    private Command findCommand() {
        Command cmd = commands.get(cmdName);
        if (cmd == null) {
            throw new IllegalArgumentException("Unrecognized command " + cmdName);
        }
        return cmd;
    }

    protected Options buildOptions(Command command) {
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("This help information").build());

        switch (command.getCommandType()) {
            case NoKeySpace:
                break;
            case TargetKeySpace:
                options.addOption(Option.builder("t").longOpt("target").hasArg().required().desc("Target keyspace name").build());
                break;
            case SourceTargetKeySpaces:
                options.addOption(Option.builder("s").longOpt("source").hasArg().required().desc("Source keyspace name").build());
                options.addOption(Option.builder("t").longOpt("target").hasArg().required().desc("Target keyspace name").build());
                break;
        }

        // Merge common options with command specific ones
        command.getOptions().getOptions().forEach(options::addOption);

        return options;
    }

    private boolean hasHelpOption(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-h") || args[i].equals("--help")) {
                return true;
            }
        }
        return false;
    }

    private CommandLine parseOptions(String[] args, Options options) {
        CommandLineParser parser = new DefaultParser();
        CommandLine cli;
        try {
            cli = parser.parse(options, args, true);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        if (cli.getArgList().size() > 0) {
            throw new IllegalArgumentException("Too many command line arguments: " + cli.getArgList());
        }
        return cli;
    }

    private void printHelp() {
        PrintWriter writer = new PrintWriter(System.out);
        HelpFormatter formatter = new HelpFormatter();

        writer.println("Usage: CassTool <cmd> [<option1>... ]");
        writer.println();
        writer.println("Commands:");

        commands.forEach((name, cmd) -> {
            writer.println(name + ": " + cmd.getDescription());
            formatter.printOptions(writer, 128, buildOptions(cmd), 4, 4);
            writer.println();
        });
        writer.flush();
    }
}
