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

package com.netflix.titus.testkit.rx;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;

public class TitusRxSubscriber<T> implements Subscriber<T>, Disposable {

    private static final Object COMPLETED_MARKER = "CompletedMarker";
    private static final Object ERROR_MARKER = "ErrorMarker";

    private final List<Object> emits = new CopyOnWriteArrayList<>();
    private final BlockingQueue<T> items = new LinkedBlockingQueue<>();
    private final ReentrantLock lock = new ReentrantLock();

    private volatile Subscription subscription;
    private volatile Throwable error;
    private volatile boolean disposed;

    private final Object eventWaitLock = new Object();

    @Override
    public void onSubscribe(Subscription s) {
        subscription = s;
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T value) {
        tryEmit(value);
        notifyClients();
    }

    @Override
    public void onError(Throwable t) {
        tryEmit(ERROR_MARKER);
        this.error = t;
        notifyClients();
    }

    @Override
    public void onComplete() {
        tryEmit(COMPLETED_MARKER);
        notifyClients();
    }

    @Override
    public void dispose() {
        if (subscription != null) {
            subscription.cancel();
            this.disposed = true;
        }
        notifyClients();
    }

    @Override
    public boolean isDisposed() {
        return disposed;
    }

    public boolean isOpen() {
        return emits.isEmpty() || !isMarker(last());
    }

    public boolean hasError() {
        return !isOpen() && last() == ERROR_MARKER;
    }

    public Throwable getError() {
        Preconditions.checkState(hasError(), "Subscription not terminated with an error");
        return error;
    }

    public List<T> getAllItems() {
        return (List<T>) emits.stream().filter(v -> !isMarker(v)).collect(Collectors.toList());
    }

    public T takeNext() {
        return afterTakeNext(items.poll());
    }

    public T takeNext(Duration duration) throws InterruptedException {
        return afterTakeNext(items.poll(duration.toMillis(), TimeUnit.MILLISECONDS));
    }

    public T takeUntil(Predicate<T> predicate, Duration duration) throws InterruptedException {
        long deadline = System.currentTimeMillis() + duration.toMillis();
        while (deadline > System.currentTimeMillis()) {
            long leftTime = deadline - System.currentTimeMillis();
            if (leftTime <= 0) {
                return null;
            }
            T next = afterTakeNext(items.poll(leftTime, TimeUnit.MILLISECONDS));
            if (next == null) {
                return null;
            }
            if (predicate.apply(next)) {
                return next;
            }
        }
        return null;
    }

    public List<T> takeNext(int n, Duration timeout) throws InterruptedException, IllegalStateException {
        List<T> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            T next = takeNext(timeout);
            if (next == null) {
                if (result.size() != n) {
                    throw new IllegalStateException("Did not receive the required number of items: " + n + ", only received: " + result.size());
                }
                break;
            }
            result.add(next);
        }
        return result;
    }

    private void notifyClients() {
        synchronized (eventWaitLock) {
            eventWaitLock.notifyAll();
        }
    }

    public void failIfClosed() {
        if (isOpen()) {
            return;
        }
        if (error == null) {
            throw new IllegalStateException("Stream completed");
        }
        throw new IllegalStateException("Stream terminated with an error", error);
    }

    public boolean awaitClosed(Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (isOpen() && (deadline - System.currentTimeMillis()) > 0) {
            synchronized (eventWaitLock) {
                if (isOpen() && (deadline - System.currentTimeMillis()) > 0) {
                    eventWaitLock.wait(deadline - System.currentTimeMillis());
                }
            }
        }
        return !isOpen();
    }

    private T afterTakeNext(T value) {
        if (value == COMPLETED_MARKER) {
            return null;
        }
        if (value == ERROR_MARKER) {
            throw new RuntimeException(error);
        }
        return value;
    }

    private void tryEmit(Object value) {
        Preconditions.checkState(lock.tryLock(), "Concurrent emits");
        try {
            Preconditions.checkState(isOpen(), "Concurrent emits");
            emits.add(value);
            if (!isMarker(value)) {
                items.add((T) value);
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean isMarker(Object value) {
        return value == COMPLETED_MARKER || value == ERROR_MARKER;
    }

    private Object last() {
        return emits.get(emits.size() - 1);
    }
}
