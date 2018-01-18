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

/**
 * Command to truncate job tables. This command can be run only for development or backup keyspaces.
 */
public class JobTruncateCommand implements Command {

    @Override
    public String getDescription() {
        return "Truncate jobs tables";
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.TargetKeySpace;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("a").longOpt("archive").desc("Truncate archive tables").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) {
        String targetKeySpace = context.getTargetSession().getLoggedKeyspace();

        Preconditions.checkArgument(
                StorePreconditions.isDevOrBackupStack(targetKeySpace),
                "Can truncate non development/backup keyspaces: " + targetKeySpace
        );

        boolean includeArchive = context.getCommandLine().hasOption('a');

        List<String> titusTables = includeArchive ? CassandraSchemas.JOB_TABLES : CassandraSchemas.JOB_ACTIVE_TABLES;
        titusTables.forEach(table -> CassandraUtils.truncateTable(context, table));
    }
}
