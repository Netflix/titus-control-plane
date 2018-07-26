package com.netflix.titus.runtime.connector.common.replicator;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;

public abstract class AbstractReplicatorEventStream<D> implements ReplicatorEventStream<D> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractReplicatorEventStream.class);

    protected final DataReplicatorMetrics metrics;
    protected final TitusRuntime titusRuntime;
    protected final Scheduler scheduler;

    protected AbstractReplicatorEventStream(DataReplicatorMetrics metrics, TitusRuntime titusRuntime, Scheduler scheduler) {
        this.metrics = metrics;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;
    }

    @Override
    public Observable<ReplicatorEvent<D>> connect() {
        return newConnection()
                .compose(ObservableExt.reemiter(
                        // If there are no events in the stream, we will periodically emit the last cache instance
                        // with the updated cache update timestamp, so it does not look stale.
                        cacheEvent -> new ReplicatorEvent<>(cacheEvent.getData(), titusRuntime.getClock().wallTime()),
                        LATENCY_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS,
                        scheduler
                ))
                .doOnNext(event -> {
                    metrics.connected();
                    metrics.event(titusRuntime.getClock().wallTime() - event.getLastUpdateTime());
                })
                .doOnUnsubscribe(metrics::disconnected)
                .doOnError(error -> {
                    logger.warn("[{}] Connection to the event stream terminated with an error: {}", getClass().getSimpleName(), error.getMessage(), error);
                    metrics.disconnected(error);
                })
                .doOnCompleted(metrics::disconnected);
    }

    protected abstract Observable<ReplicatorEvent<D>> newConnection();
}
