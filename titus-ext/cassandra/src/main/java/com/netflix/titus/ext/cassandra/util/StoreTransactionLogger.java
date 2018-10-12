package com.netflix.titus.ext.cassandra.util;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Help class to write store (CRUD) transaction log.
 */
public class StoreTransactionLogger {

    private static final Logger logger = LoggerFactory.getLogger("CassandraStoreTransactionLogger");

    private static final int SUMMARY_MAX_SIZE = 128;

    private final Map<Class<?>, Function<Object, String>> entityKeySelectors;
    private final Map<Class<?>, Function<Object, String>> entityFormatters;

    private StoreTransactionLogger(Map<Class<?>, Function<Object, String>> entityKeySelectors,
                                   Map<Class<?>, Function<Object, String>> entityFormatters) {
        this.entityKeySelectors = entityKeySelectors;
        this.entityFormatters = entityFormatters;
    }

    public <T> void logBeforeCreate(PreparedStatement statement, String method, T entity) {
        log(statement, method, entity, "BeforeCreate");
    }

    public <T> void logAfterCreate(PreparedStatement statement, String method, T entity) {
        log(statement, method, entity, "AfterCreate");
    }

    public <T> void logBeforeUpdate(PreparedStatement statement, String method, T entity) {
        log(statement, method, entity, "BeforeUpdate");
    }

    public <T> void logAfterUpdate(PreparedStatement statement, String method, T entity) {
        log(statement, method, entity, "AfterUpdate");
    }

    public void logBeforeRead(PreparedStatement statement, String key, String method) {
        tryLog(() -> doFormat(key, statement.getQueryKeyspace(), method, "BeforeRead", statement.getConsistencyLevel(), "<N/A>"));
    }

    public <T> void logAfterRead(PreparedStatement statement, String method, T entity) {
        log(statement, method, entity, "AfterRead");
    }

    public <T> void logBeforeDelete(PreparedStatement statement, String method, T entity) {
        log(statement, method, entity, "BeforeDelete");
    }

    public <T> void logAfterDelete(PreparedStatement statement, String method, T entity) {
        log(statement, method, entity, "AfterDelete");
    }

    private <T> void log(PreparedStatement statement, String operation, T entity, String crudAction) {
        tryLog(() -> doFormat(doFormatKey(entity), statement.getQueryKeyspace(), operation, crudAction, statement.getConsistencyLevel(), doFormatEntity(entity)));
    }

    private void tryLog(Supplier<String> messageProducer) {
        try {
            logger.info(messageProducer.get());
        } catch (Exception e) {
            logger.error("Logging error: {}", e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("Stack trace", e);
            }
        }
    }

    private String doFormatKey(Object entity) {
        if (entity == null) {
            return "<null>";
        }
        Function<Object, String> keySelector = entityKeySelectors.get(entity.getClass());
        if (keySelector != null) {
            return keySelector.apply(entity);
        }

        return entity.getClass().getSimpleName() + '#' + Integer.toHexString(System.identityHashCode(entity));
    }

    private String doFormatEntity(Object entity) {
        if (entity == null) {
            return "<null>";
        }
        Function<Object, String> formatter = entityFormatters.get(entity.getClass());
        if (formatter != null) {
            return formatter.apply(entity);
        }
        return entity.toString().substring(SUMMARY_MAX_SIZE);
    }

    private String doFormat(String key,
                            String table,
                            String method,
                            String crudAction,
                            ConsistencyLevel consistencyLevel,
                            String summary) {
        return String.format(
                "key=%s, table=%-15s method=%-20s crudAction=%-8s consistencyLevel=%-5s summary=%s",
                key,
                table,
                method,
                crudAction,
                consistencyLevel,
                summary
        );
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private final Map<Class<?>, Function<Object, String>> entityKeySelectors = new HashMap<>();
        private final Map<Class<?>, Function<Object, String>> entityFormatters = new HashMap<>();

        public <T> Builder withEntityKeySelectors(Class<T> entityType, Function<T, String> entityKeySelector) {
            entityKeySelectors.put(entityType, (Function) entityKeySelector);
            return this;
        }

        public <T> Builder withEntityFormatter(Class<T> type, Function<T, String> formatter) {
            entityFormatters.put(type, (Function) formatter);
            return this;
        }

        public StoreTransactionLogger build() {
            return new StoreTransactionLogger(entityKeySelectors, entityFormatters);
        }
    }
}
