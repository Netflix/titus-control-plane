package io.netflix.titus.ext.cassandra.tool.command;

import java.util.List;

import com.google.common.base.Preconditions;
import io.netflix.titus.ext.cassandra.tool.CassandraSchemas;
import io.netflix.titus.ext.cassandra.tool.CassandraUtils;
import io.netflix.titus.ext.cassandra.tool.Command;
import io.netflix.titus.ext.cassandra.tool.CommandContext;
import io.netflix.titus.ext.cassandra.tool.StorePreconditions;
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
        options.addOption(Option.builder("c").longOpt("clean").desc("Remove the original table content before copy").build());
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
