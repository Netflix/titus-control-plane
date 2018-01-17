package io.netflix.titus.ext.cassandra.tool;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.ext.cassandra.executor.AsyncCassandraExecutor;
import org.apache.commons.cli.CommandLine;

public class DefaultCommandContext implements CommandContext {
    private static final int PAGE_SIZE = 1000;
    private static final int SPLIT = 2;

    private final CommandLine commandLine;

    private final Session defaultSession;
    private final Optional<CassSession> sourceSession;
    private final Optional<CassSession> targetSession;

    public DefaultCommandContext(CommandLine commandLine,
                                 Session defaultSession,
                                 Function<String, Session> sourceSessionFactory,
                                 Function<String, Session> targetSessionFactory) {
        this.commandLine = commandLine;
        this.defaultSession = defaultSession;
        this.sourceSession = commandLine.hasOption('s')
                ? Optional.of(new CassSession(sourceSessionFactory, commandLine.getOptionValue('s')))
                : Optional.empty();
        this.targetSession = commandLine.hasOption('t')
                ? Optional.of(new CassSession(targetSessionFactory, commandLine.getOptionValue('t')))
                : Optional.empty();
    }

    @Override
    public void shutdown() {
    }

    @Override
    public CommandLine getCommandLine() {
        return commandLine;
    }

    @Override
    public Session getDefaultSession() {
        return defaultSession;
    }

    @Override
    public Session getSourceSession() {
        Preconditions.checkState(sourceSession.isPresent(), "Cassandra source keyspace not defined");
        return sourceSession.get().getOrCreateSession();
    }

    @Override
    public Session getTargetSession() {
        Preconditions.checkState(targetSession.isPresent(), "Cassandra target keyspace not defined");
        return targetSession.get().getOrCreateSession();
    }

    @Override
    public String getSourceKeySpace() {
        Preconditions.checkState(sourceSession.isPresent(), "Cassandra source keyspace not defined");
        return sourceSession.get().getKeySpace();
    }

    @Override
    public String getTargetKeySpace() {
        Preconditions.checkState(targetSession.isPresent(), "Cassandra target keyspace not defined");
        return targetSession.get().getKeySpace();
    }

    @Override
    public AsyncCassandraExecutor getSourceCassandraExecutor() {
        Preconditions.checkState(sourceSession.isPresent(), "Cassandra source keyspace not defined");
        return sourceSession.get().getOrCreateExecutor();
    }

    @Override
    public AsyncCassandraExecutor getTargetCassandraExecutor() {
        Preconditions.checkState(targetSession.isPresent(), "Cassandra target keyspace not defined");
        return targetSession.get().getOrCreateExecutor();
    }

    public static CommandContext newCommandContext(CommandLine commandLine) {
        List<String> ips = StringExt.splitByComma(commandLine.getOptionValue("H"));
        int sourcePort = Integer.parseInt(commandLine.getOptionValue("p"));

        QueryOptions queryOptions = new QueryOptions()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        Cluster cluster = Cluster.builder()
                .addContactPoints((String[]) ips.toArray())
                .withPort(sourcePort)
                .withQueryOptions(queryOptions)
                .build();

        return new DefaultCommandContext(
                commandLine,
                cluster.newSession(),
                sourceKeySpace -> cluster.connect('"' + sourceKeySpace + '"'),
                targetKeySpace -> cluster.connect('"' + targetKeySpace + '"')
        ) {
            @Override
            public void shutdown() {
                cluster.close();
            }
        };
    }

    class CassSession {
        private final Function<String, Session> sessionFactory;
        private final String keySpace;

        private Session session;
        private AsyncCassandraExecutor executor;

        CassSession(Function<String, Session> sessionFactory, String keySpace) {
            this.sessionFactory = sessionFactory;
            this.keySpace = keySpace;
        }

        String getKeySpace() {
            return keySpace;
        }

        Session getOrCreateSession() {
            if (session == null) {
                session = sessionFactory.apply(keySpace);
            }
            return session;
        }

        AsyncCassandraExecutor getOrCreateExecutor() {
            if (executor == null) {
                executor = createCassExecutor(getOrCreateSession());
            }
            return executor;
        }

        private AsyncCassandraExecutor createCassExecutor(Session session) {
            return new AsyncCassandraExecutor(session, PAGE_SIZE, SPLIT);
        }
    }
}
