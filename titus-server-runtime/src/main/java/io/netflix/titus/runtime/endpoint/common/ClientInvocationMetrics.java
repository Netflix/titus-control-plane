package io.netflix.titus.runtime.endpoint.common;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientInvocationMetrics {

    private static final Logger logger = LoggerFactory.getLogger(ClientInvocationMetrics.class);

    private static final BasicTag SUCCESS_TAG = new BasicTag("status", "success");
    private static final BasicTag FAILURE_TAG = new BasicTag("status", "failure");

    private final String rootName;
    private final HostCallerIdResolver hostCallerIdResolver;
    private final Registry registry;

    private final ConcurrentMap<ClientKey, ClientMetrics> counterMap = new ConcurrentHashMap<>();

    public ClientInvocationMetrics(String rootName,
                                   HostCallerIdResolver hostCallerIdResolver,
                                   Registry registry) {
        this.rootName = rootName;
        this.hostCallerIdResolver = hostCallerIdResolver;
        this.registry = registry;
    }

    public void registerSuccess(String callerHost, List<Tag> tags, long durationMs) {
        register(callerHost, CollectionsExt.copyAndAdd(tags, SUCCESS_TAG), durationMs);
    }

    public void registerFailure(String callerHost, List<Tag> tags, long durationMs) {
        register(callerHost, CollectionsExt.copyAndAdd(tags, FAILURE_TAG), durationMs);
    }

    private void register(String callerHost, List<Tag> tags, long durationMs) {
        String application = hostCallerIdResolver.resolve(callerHost).orElseGet(() -> {
            logger.info("Cannot identify source with host address {}", callerHost);
            return "UNKNOWN";
        });
        ClientKey clientKey = new ClientKey(application, tags);
        ClientMetrics clientMetrics = counterMap.computeIfAbsent(clientKey, ClientMetrics::new);
        clientMetrics.registerInvocation(durationMs);
    }

    private static class ClientKey {
        private final String application;
        private final List<Tag> tags;

        private ClientKey(String application, List<Tag> tags) {
            this.application = application;
            this.tags = tags;
        }

        private String getApplication() {
            return application;
        }

        private List<Tag> getTags() {
            return tags;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ClientKey clientKey = (ClientKey) o;

            if (application != null ? !application.equals(clientKey.application) : clientKey.application != null) {
                return false;
            }
            return tags != null ? tags.equals(clientKey.tags) : clientKey.tags == null;
        }

        @Override
        public int hashCode() {
            int result = application != null ? application.hashCode() : 0;
            result = 31 * result + (tags != null ? tags.hashCode() : 0);
            return result;
        }
    }

    private class ClientMetrics {

        private final Counter counter;
        private final Timer latency;

        private ClientMetrics(ClientKey clientKey) {
            List<Tag> tags = CollectionsExt.copyAndAdd(clientKey.getTags(), new BasicTag("application", clientKey.getApplication()));
            this.counter = registry.counter(rootName + "requests", tags);
            this.latency = registry.timer(rootName + "latency", tags);
        }

        private void registerInvocation(long durationMs) {
            counter.increment();
            latency.record(durationMs, TimeUnit.MILLISECONDS);
        }
    }
}
