/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.common.util.rx.eventbus.internal;

import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.rx.eventbus.RxEventBus;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Producer;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.internal.operators.BackpressureUtils;
import rx.schedulers.Schedulers;

public class DefaultRxEventBus implements RxEventBus {

    private static final Logger logger = LoggerFactory.getLogger(DefaultRxEventBus.class);

    private static final long MAX_QUEUE_SiZE = 10000;

    private final long maxQueueSize;
    private final Scheduler.Worker worker;
    private final RxEventBusMetrics metrics;

    private final Set<SubscriptionHandler> subscriptionHandlers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public DefaultRxEventBus(Id rootId, Registry registry) {
        this(rootId, registry, MAX_QUEUE_SiZE, Schedulers.computation());
    }

    public DefaultRxEventBus(Id rootId, Registry registry, long maxQueueSize, Scheduler scheduler) {
        this.maxQueueSize = maxQueueSize;
        this.worker = scheduler.createWorker();
        this.metrics = new RxEventBusMetrics(rootId, registry);
    }

    @Override
    public void close() {
        if (worker.isUnsubscribed() && subscriptionHandlers.isEmpty()) {
            return;
        }
        logger.debug("Closing EventBus");
        subscriptionHandlers.forEach(SubscriptionHandler::close);
        subscriptionHandlers.clear();
        worker.unsubscribe();
    }

    @Override
    public <E> void publish(E event) {
        checkIfOpen();

        logger.debug("Publishing event {}", event);
        publish(new Pair<>(worker.now(), event));
        metrics.published(event);
    }

    @Override
    public <E> void publishAsync(E event) {
        checkIfOpen();

        logger.debug("Publishing event {}", event);
        worker.schedule(() -> publish(new Pair<>(worker.now(), event)));
        metrics.published(event);
    }

    private void publish(Pair<Long, Object> eventWithTimestamp) {
        for (SubscriptionHandler handler : subscriptionHandlers) {
            if (!handler.isUnsubscribed()) {
                handler.publish(eventWithTimestamp);
            }
        }
    }

    private void checkIfOpen() {
        if (worker.isUnsubscribed()) {
            throw new IllegalStateException("EventBus closed");
        }
    }

    @Override
    public <E> Observable<E> listen(String subscriberId, Class<E> eventType) {
        return Observable.create(subscriber -> {
            logger.debug("Subscribed {} for event {}", subscriberId, eventType.getName());

            // We register cleanup hook in SubscriptionHandler constructor, so we need to check for early unsubscribe
            SubscriptionHandler handler = new SubscriptionHandler(subscriberId, eventType, (Subscriber<Object>) subscriber);
            if (!handler.isUnsubscribed()) {
                subscriptionHandlers.add(handler);
                if (handler.isUnsubscribed()) {
                    subscriptionHandlers.remove(handler);
                }
            }
        });
    }

    /**
     * Drain method implementation based on RxJava guidelines
     * (see https://github.com/ReactiveX/RxJava/wiki/Implementing-custom-operators-(draft)).
     */
    private class SubscriptionHandler implements Subscription, Producer {

        private final String subscriberId;
        private final Class<?> eventType;
        private final Subscriber<Object> subscriber;

        private final AtomicInteger queueSize = new AtomicInteger();
        private final Queue<Pair<Long, Object>> eventQueue = new ConcurrentLinkedQueue<>();

        // mutual exclusion
        private final AtomicInteger counter = new AtomicInteger();
        // tracks the downstream request amount
        private final AtomicLong requested = new AtomicLong();

        // no more values expected from upstream
        private volatile boolean done;

        // the upstream error
        private volatile Throwable error;

        SubscriptionHandler(String subscriberId, Class<?> eventType, Subscriber<Object> subscriber) {
            this.subscriberId = subscriberId;
            this.eventType = eventType;
            this.subscriber = subscriber;

            subscriber.add(this);
            subscriber.setProducer(this);

            metrics.subscriberAdded(subscriberId);
        }

        void publish(Pair<Long, Object> eventWithTimestamp) {
            Object event = eventWithTimestamp.getRight();
            if (!subscriber.isUnsubscribed() && eventType.isAssignableFrom(event.getClass())) {
                if (queueSize.incrementAndGet() > maxQueueSize) {
                    error = new IllegalStateException("Event queue overflow");
                    metrics.overflowed(subscriberId);
                    done = true;
                } else {
                    eventQueue.add(eventWithTimestamp);
                }
                drain();
            }
        }

        void close() {
            done = true;
            drain();
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                BackpressureUtils.getAndAddRequest(requested, n);
                drain();
            }
        }

        @Override
        public void unsubscribe() {
            subscriptionHandlers.remove(this);
            logger.debug("Unsubscribed {} for event {}", subscriberId, eventType.getName());
        }

        @Override
        public boolean isUnsubscribed() {
            return subscriber.isUnsubscribed();
        }

        private void drain() {
            if (counter.getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            for (; ; ) {
                // Error happens only when we have overflow, in which case we ignore all elements in the queue.
                if (error != null) {
                    terminate();
                    return;
                }

                long requests = requested.get();
                long emission = 0L;

                while (emission != requests) { // don't emit more than requested
                    if (subscriber.isUnsubscribed()) {
                        return;
                    }

                    // Error happens only when we have overflow, in which case we ignore all elements in the queue.
                    if (error != null) {
                        terminate();
                        return;
                    }

                    boolean stop = done;  // order matters here!
                    Pair<Long, Object> eventWithTimestamp = eventQueue.poll();
                    boolean empty = eventWithTimestamp == null;

                    // if no more values, emit completion event
                    if (stop && empty) {
                        terminate();
                        return;
                    }
                    // the upstream hasn't stopped yet but we don't have a value available
                    if (empty) {
                        break;
                    }

                    Object event = eventWithTimestamp.getRight();
                    long latency = worker.now() - eventWithTimestamp.getLeft();

                    int currentQueueSize = queueSize.decrementAndGet();
                    subscriber.onNext(event);
                    metrics.delivered(subscriberId, currentQueueSize, event, latency);
                    emission++;

                    logger.debug("Emitted event {} to subscriber {}", event, subscriberId);
                }

                // if we are at a request boundary, a terminal event can be still emitted without requests
                if (emission == requests) {
                    if (subscriber.isUnsubscribed()) {
                        return;
                    }

                    boolean stop = done;  // order matters here!
                    boolean empty = eventQueue.isEmpty();

                    // if no more values, emit completion event
                    if (stop && empty) {
                        terminate();
                        return;
                    }
                }

                // decrement the current request amount by the emission count
                if (emission != 0L && requests != Long.MAX_VALUE) {
                    BackpressureUtils.produced(requested, emission);
                }

                // indicate that we have performed the outstanding amount of work
                missed = counter.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
                // if a concurrent getAndIncrement() happened, we loop back and continue
            }
        }

        private void terminate() {
            Throwable ex = error;
            if (ex != null) {
                subscriber.onError(ex);
                logger.debug("Completed {}/{} subscription with error", subscriberId, eventType.getName(), ex);
            } else {
                subscriber.onCompleted();
                logger.debug("Completed {}/{} subscription", subscriberId, eventType.getName());
            }
            metrics.subscriberRemoved(subscriberId);
        }
    }
}
