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

package com.netflix.titus.testkit.grpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.rpc.BadRequest;
import com.netflix.titus.client.common.grpc.GrpcClientErrorUtils;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.runtime.endpoint.v3.grpc.ErrorResponses;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import rx.Observable;
import rx.subjects.PublishSubject;

/**
 * GRPC {@link io.grpc.stub.StreamObserver} implementation for testing.
 */
public class TestStreamObserver<T> extends ServerCallStreamObserver<T> {

    private static final Object EOS_MARKER = new Object();

    private final List<T> emittedItems = new CopyOnWriteArrayList<>();

    private final BlockingQueue<T> availableItems = new LinkedBlockingQueue<>();
    private final PublishSubject<T> eventSubject = PublishSubject.create();

    private final CountDownLatch terminatedLatch = new CountDownLatch(1);
    private volatile Throwable error;
    private volatile RuntimeException mappedError;
    private volatile boolean completed;

    private Runnable onCancelHandler;
    private boolean cancelled;

    @Override
    public void onNext(T value) {
        emittedItems.add(value);
        availableItems.add(value);
        eventSubject.onNext(value);
    }

    @Override
    public void onError(Throwable error) {
        this.error = error;
        this.mappedError = exceptionMapper(error);
        eventSubject.onError(error);
        doFinish();
    }

    @Override
    public void onCompleted() {
        completed = true;
        eventSubject.onCompleted();
        doFinish();
    }

    public Observable<T> toObservable() {
        return eventSubject.compose(ObservableExt.head(() -> emittedItems));
    }

    private void doFinish() {
        availableItems.add((T) EOS_MARKER);
        terminatedLatch.countDown();

    }

    public List<T> getEmittedItems() {
        return new ArrayList<>(emittedItems);
    }

    public T getLast() throws Exception {
        terminatedLatch.await();
        throwIfError();
        if (isTerminated() && !emittedItems.isEmpty()) {
            return emittedItems.get(0);
        }
        throw new IllegalStateException("No item emitted by the stream");
    }

    public T getLast(long timout, TimeUnit timeUnit) throws InterruptedException {
        terminatedLatch.await(timout, timeUnit);
        return isTerminated() && !emittedItems.isEmpty() ? emittedItems.get(0) : null;
    }

    public T takeNext() {
        T next = availableItems.poll();
        if ((next == null || next == EOS_MARKER) && isTerminated()) {
            throw new IllegalStateException("Stream is already closed");
        }
        return next;
    }

    public T takeNext(long timeout, TimeUnit timeUnit) throws InterruptedException, IllegalStateException {
        if (isTerminated()) {
            return takeNext();
        }
        T next = availableItems.poll(timeout, timeUnit);
        if (next == EOS_MARKER) {
            throwIfError();
            throw new IllegalStateException("Stream is already closed");
        }
        return next;
    }

    public boolean isTerminated() {
        return completed || error != null;
    }

    public boolean isCompleted() {
        return completed;
    }

    public boolean hasError() {
        return error != null;
    }

    public Throwable getError() {
        Preconditions.checkState(error != null, "Error not emitted");
        return error;
    }

    public Throwable getMappedError() {
        Preconditions.checkState(error != null, "Error not emitted");
        return mappedError;
    }

    public void awaitDone() throws InterruptedException {
        terminatedLatch.await();
        if (hasError()) {
            throw new IllegalStateException("GRPC stream terminated with an error", error);
        }
    }

    public void awaitDone(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (!terminatedLatch.await(timeout, timeUnit)) {
            throw new IllegalStateException("GRPC request not completed in time");
        }
        if (hasError()) {
            throw new IllegalStateException("GRPC stream terminated with an error", error);
        }
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setOnCancelHandler(Runnable onCancelHandler) {
        this.onCancelHandler = onCancelHandler;
    }

    @Override
    public void setCompression(String compression) {
    }

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {
    }

    @Override
    public void disableAutoInboundFlowControl() {
    }

    public void cancel() {
        this.cancelled = true;
        if (onCancelHandler != null) {
            onCancelHandler.run();
        }
    }

    @Override
    public void request(int count) {
    }

    @Override
    public void setMessageCompression(boolean enable) {
    }

    private RuntimeException exceptionMapper(Throwable error) {
        if (error instanceof StatusRuntimeException) {
            StatusRuntimeException e = (StatusRuntimeException) error;
            String errorMessage = "GRPC status " + e.getStatus() + ": " + e.getTrailers().get(ErrorResponses.KEY_TITUS_ERROR_REPORT);

            Optional<BadRequest> badRequest = GrpcClientErrorUtils.getDetail(e, BadRequest.class);
            if (badRequest.isPresent()) {
                return new RuntimeException(errorMessage + ". Invalid field values: " + badRequest, error);
            }
            return new RuntimeException(errorMessage, error);
        }
        return new RuntimeException(error.getMessage(), error);
    }

    private void throwIfError() throws RuntimeException {
        if (error != null) {
            throw mappedError;
        }
    }
}
