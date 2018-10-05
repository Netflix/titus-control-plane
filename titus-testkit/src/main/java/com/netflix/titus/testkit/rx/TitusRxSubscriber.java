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

    @Override
    public void onSubscribe(Subscription s) {
        subscription = s;
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T value) {
        tryEmit(value);
    }

    @Override
    public void onError(Throwable t) {
        tryEmit(ERROR_MARKER);
        this.error = t;
    }

    @Override
    public void onComplete() {
        tryEmit(COMPLETED_MARKER);
    }

    @Override
    public void dispose() {
        if (subscription != null) {
            subscription.cancel();
            this.disposed = true;
        }
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

    public T takeNext(Duration duration) throws InterruptedException {
        return afterTakeNext(items.poll(duration.toMillis(), TimeUnit.MILLISECONDS));
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

    public void failIfClosed() {
        if (isOpen()) {
            return;
        }
        if (error == null) {
            throw new IllegalStateException("Stream completed");
        }
        throw new IllegalStateException("Stream terminated with an error", error);
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
