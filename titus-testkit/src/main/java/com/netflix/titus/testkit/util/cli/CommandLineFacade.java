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

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CommandLineFacade {

    private final Options options;
    private final CommandLine apacheCli;
    private final Map<String, Object> defaults;
    private final boolean includesHelpOption;

    CommandLineFacade(Options options, CommandLine apacheCli, Map<String, Object> defaults, boolean helpOption) {
        this.options = options;
        this.apacheCli = apacheCli;
        this.defaults = defaults;
        this.includesHelpOption = helpOption;
    }

    CommandLineFacade(Options options) {
        this.options = options;
        this.apacheCli = null;
        this.defaults = Collections.emptyMap();
        this.includesHelpOption = true;
    }

    public boolean hasHelpOption() {
        return includesHelpOption;
    }

    public String getString(String optionName) {
        String value = apacheCli.getOptionValue(optionName);
        if (value == null && defaults.get(optionName) != null) {
            return defaults.get(optionName).toString();
        }
        return value;
    }

    public int getInt(String optionName) {
        Object value;
        try {
            value = apacheCli.getParsedOptionValue(optionName);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
        if (value == null) {
            return (int) defaults.get(optionName);
        }
        long longValue = (long) value;
        return (int) longValue;
    }

    public boolean isEnabled(String optionName) {
        return apacheCli.hasOption(optionName);
    }

    public void printHelp(String application) {
        PrintWriter writer = new PrintWriter(System.out);
        HelpFormatter formatter = new HelpFormatter();

        writer.println(String.format("Usage: %s [params]", application));
        writer.println();
        writer.println("Options");
        formatter.printOptions(writer, 128, options, 4, 4);
        writer.println();
        writer.flush();
    }
}
