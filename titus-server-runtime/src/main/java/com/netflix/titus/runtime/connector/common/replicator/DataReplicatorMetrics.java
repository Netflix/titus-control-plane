package com.netflix.titus.runtime.connector.common.replicator;

import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.TitusRuntime;

/**
 * Recommended metrics that should be reported by {@link DataReplicator} implementations.
 */
public class DataReplicatorMetrics {

    private static final String ROOT = "titus.dataReplicator.";

    private final Registry registry;

    private final AtomicLong connected = new AtomicLong();
    private final Id failuresId;
    private final Gauge staleness;

    public DataReplicatorMetrics(String source, TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();

        // Use PolledMeter as this metric is set infrequently
        PolledMeter.using(registry).withId(registry.createId(ROOT + "connected", "source", source)).monitorValue(connected);

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
