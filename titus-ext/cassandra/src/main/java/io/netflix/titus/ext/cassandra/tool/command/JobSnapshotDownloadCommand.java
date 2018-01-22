package io.netflix.titus.ext.cassandra.tool.command;

import java.io.File;

import io.netflix.titus.ext.cassandra.tool.Command;
import io.netflix.titus.ext.cassandra.tool.CommandContext;
import io.netflix.titus.ext.cassandra.tool.snapshot.JobSnapshotDownloader;
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
