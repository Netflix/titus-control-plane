package com.netflix.titus.runtime.connector.common.replicator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;

public abstract class StreamDataReplicator<D> implements DataReplicator<D> {
    private static final Logger logger = LoggerFactory.getLogger(StreamDataReplicator.class);

    // Staleness threshold checked during the system initialization.
    private static final long STALENESS_THRESHOLD = 60_000;

    private static final long INITIALIZATION_TIMEOUT_MS = 120_000;

    private final TitusRuntime titusRuntime;
    private final Subscription subscription;

    private final AtomicReference<ReplicatorEventStream.ReplicatorEvent<D>> lastReplicatorEventRef = new AtomicReference<>();

    private final PublishSubject<Long> stalenessSubject = PublishSubject.create();
    private final Observable<Long> stalenessObservable = ObservableExt.protectFromMissingExceptionHandlers(stalenessSubject.asObservable(), logger);

    public StreamDataReplicator(ReplicatorEventStream<D> replicatorEventStream,
                                DataReplicatorMetrics metrics,
                                TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;

        CountDownLatch latch = new CountDownLatch(1);

        this.subscription = replicatorEventStream.connect()
                .doOnSubscribe(metrics::connected)
                .doOnUnsubscribe(metrics::disconnected)
                .subscribe(
                        next -> {
                            lastReplicatorEventRef.set(next);
                            if (latch.getCount() > 0 && isFresh(next)) {
                                latch.countDown();
                            }
                            stalenessSubject.onNext(next.getLastUpdateTime());
                            metrics.event(titusRuntime.getClock().wallTime() - next.getLastUpdateTime());
                        },
                        e -> {
                            logger.error("Unexpected error in the replicator event stream", e);
                            metrics.disconnected(e);
                        },
                        () -> {
                            logger.info("Replicator event stream completed");
                            metrics.disconnected();
                        }
                );

        try {
            if (!latch.await(INITIALIZATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Replicator event stream initialization timeout");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Cannot initialize the replicator event cache", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        subscription.unsubscribe();
    }

    @Override
    public D getCurrent() {
        return lastReplicatorEventRef.get().getData();
    }

    @Override
    public long getStalenessMs() {
        return titusRuntime.getClock().wallTime() - lastReplicatorEventRef.get().getLastUpdateTime();
    }

    @Override
    public Observable<Long> observeDataStalenessMs() {
        return stalenessObservable;
    }

    private boolean isFresh(ReplicatorEventStream.ReplicatorEvent event) {
        long now = titusRuntime.getClock().wallTime();
        return event.getLastUpdateTime() + STALENESS_THRESHOLD >= now;
    }
}
