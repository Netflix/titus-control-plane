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

package com.netflix.titus.testkit.util.cli;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CommandLineBuilder {

    private final Options options = new Options();
    private final Map<String, Object> defaults = new HashMap<>();

    public CommandLineBuilder withHostAndPortOption(String target) {
        options.addOption(Option.builder("H").longOpt("host").argName("host_name").hasArg()
                .desc(String.format("%s host name", target))
                .build());
        options.addOption(Option.builder("p").longOpt("port").argName("port_number").hasArg().type(Number.class)
                .desc(String.format("%s port number", target))
                .build());

        return this;
    }

    public CommandLineBuilder withHostAndPortOption(String target, String defaultHost, int defaultPort) {
        options.addOption(Option.builder("H").longOpt("host").argName("host_name").hasArg()
                .desc(String.format("%s host name (default %s)", target, defaultHost))
                .build());
        options.addOption(Option.builder("p").longOpt("port").argName("port_number").hasArg().type(Number.class)
                .desc(String.format("%s port number (default %s)", target, defaultPort))
                .build());

        defaults.put("H", defaultHost);
        defaults.put("p", defaultPort);

        return this;
    }

    public CommandLineBuilder withOption(Option option) {
        this.options.addOption(option);
        return this;
    }

    public CommandLineFacade build(String[] args) {
        if(hasHelpOption(args)) {
            return new CommandLineFacade(options);
        }

        CommandLineParser parser = new DefaultParser();
        try {
            return new CommandLineFacade(options, parser.parse(options, args), defaults, hasHelpOption(args));
        } catch (ParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public static CommandLineBuilder newApacheCli() {
        return new CommandLineBuilder();
    }

    private static boolean hasHelpOption(String[] args) {
        for (String arg : args) {
            if (arg.equals("-h") || arg.equals("--help")) {
                return true;
            }
        }
        return false;
    }
}
