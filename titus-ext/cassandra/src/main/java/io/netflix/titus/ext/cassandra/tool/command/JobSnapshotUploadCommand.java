package io.netflix.titus.ext.cassandra.tool.command;

import java.io.File;

import io.netflix.titus.ext.cassandra.tool.Command;
import io.netflix.titus.ext.cassandra.tool.CommandContext;
import io.netflix.titus.ext.cassandra.tool.snapshot.JobSnapshotLoader;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class JobSnapshotUploadCommand implements Command {

    @Override
    public String getDescription() {
        return "Loads job snapshot into Cassandra";
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.TargetKeySpace;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i")
                .longOpt("input folder")
                .required()
                .hasArg()
                .desc("Input folder containing snapshot files")
                .build()
        );
        return options;
    }

    @Override
    public void execute(CommandContext context) {
        File output = new File(context.getCommandLine().getOptionValue('i'));
        JobSnapshotLoader downloader = new JobSnapshotLoader(context.getTargetSession(), output);
        downloader.load();
    }
}
