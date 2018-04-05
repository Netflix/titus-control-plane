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

import java.io.File;

import com.netflix.titus.ext.cassandra.tool.Command;
import com.netflix.titus.ext.cassandra.tool.CommandContext;
import com.netflix.titus.ext.cassandra.tool.snapshot.JobSnapshotDownloader;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class JobSnapshotDownloadCommand implements Command {

    @Override
    public String getDescription() {
        return "Save job tables content to a disk in JSON format";
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.TargetKeySpace;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("a").longOpt("archive").desc("Include archived tables").build());
        options.addOption(Option.builder("o")
                .longOpt("output folder")
                .required()
                .hasArg()
                .desc("Output folder for snapshot files")
                .build()
        );
        return options;
    }

    @Override
    public void execute(CommandContext context) {
        File output = new File(context.getCommandLine().getOptionValue('o'));
        boolean includeArchived = context.getCommandLine().hasOption('a');
        JobSnapshotDownloader downloader = new JobSnapshotDownloader(context.getTargetSession(), includeArchived, output);
        downloader.download();
    }
}
