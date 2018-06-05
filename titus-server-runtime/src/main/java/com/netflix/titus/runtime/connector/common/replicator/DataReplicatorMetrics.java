package com.netflix.titus.runtime.connector.common.replicator;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.runtime.TitusRuntime;

/**
 * Recommended metrics that should be reported by {@link DataReplicator} implementations.
 */
public class DataReplicatorMetrics {

    private static final String ROOT = "titus.dataReplicator.";

    private final Registry registry;

    private final Gauge connected;
    private final Id failuresId;
    private final Gauge staleness;

    public DataReplicatorMetrics(String source, TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();

        this.connected = registry.gauge(ROOT + "connected", "source", source);
        this.failuresId = registry.createId(ROOT + "failures", "source", source);
        this.staleness = registry.gauge(ROOT + "staleness", "source", source);
    }

    public void connected() {
        connected.set(1);
    }

    public void disconnected() {
        connected.set(0);
        staleness.set(0);
    }

    public void disconnected(Throwable error) {
        disconnected();
        registry.counter(failuresId.withTags("error", error.getClass().getSimpleName())).increment();
    }

    public void event(long dataStalenessMs) {
        staleness.set(dataStalenessMs);
    }
}
