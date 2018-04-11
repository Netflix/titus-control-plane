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

package com.netflix.titus.common.util.proxy.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.proxy.LoggingProxyBuilder;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import rx.Completable;
import rx.Observable;

/**
 * Method invocations logger.
 */
public class LoggingInvocationHandler<API, NATIVE> extends InterceptingInvocationHandler<API, NATIVE, Long> {

    private static final int MAX_CONTENT_LENGTH = 512;

    private static final int MAX_EMITTED_ITEMS_BUFFER = 10;

    private final LoggingProxyBuilder.Priority requestLevel;
    private final LoggingProxyBuilder.Priority replyLevel;
    private final LoggingProxyBuilder.Priority observableReplyLevel;
    private final LoggingProxyBuilder.Priority exceptionLevel;
    private final LoggingProxyBuilder.Priority observableErrorLevel;
    private final Logger logger;
    private final Clock clock;

    public LoggingInvocationHandler(Class<API> apiInterface,
                                    Logger logger,
                                    LoggingProxyBuilder.Priority requestLevel,
                                    LoggingProxyBuilder.Priority replyLevel,
                                    LoggingProxyBuilder.Priority observableReplyLevel,
                                    LoggingProxyBuilder.Priority exceptionLevel,
                                    LoggingProxyBuilder.Priority observableErrorLevel,
                                    TitusRuntime titusRuntime) {
        super(apiInterface, observableReplyLevel != LoggingProxyBuilder.Priority.NEVER);
        this.logger = logger;
        this.requestLevel = requestLevel;
        this.replyLevel = replyLevel;
        this.observableReplyLevel = observableReplyLevel;
        this.exceptionLevel = exceptionLevel;
        this.observableErrorLevel = observableErrorLevel;
        this.clock = titusRuntime.getClock();
    }

    @Override
    protected Long before(Method method, Object[] args) {
        logWithPriority(requestLevel, () -> {
            StringBuilder sb = new StringBuilder("Starting ").append(getMethodSignature(method));
            if (args == null || args.length == 0) {
                sb.append("()");
            } else {
                sb.append('(');
                for (int i = 0; i < args.length; i++) {
                    if (i != 0) {
                        sb.append(',');
                    }
                    sb.append(args[i]);
                }
                sb.append(')');
            }
            return sb;
        });
        return clock.wallTime();
    }

    @Override
    protected void after(Method method, Object result, Long startTime) {
        logWithPriority(replyLevel, () -> {
            StringBuilder sb = new StringBuilder("Returned from ").append(getMethodSignature(method));
            sb.append(" after ").append(clock.wallTime() - startTime).append("[ms]");
            if (result == null) {
                if (void.class.equals(method.getReturnType())) {
                    sb.append(": void");
                } else {
                    sb.append(": null");
                }
            } else {
                sb.append(": ").append(result.toString());
            }
            return sb;
        });
    }

    @Override
    protected void afterException(Method method, Throwable cause, Long startTime) {
        logWithPriority(exceptionLevel, () -> {
            Throwable realCause = cause instanceof InvocationTargetException ? cause.getCause() : cause;
            StringBuilder sb = new StringBuilder().append("Exception throw in ").append(getMethodSignature(method));
            sb.append(" after ").append(clock.wallTime() - startTime).append("[ms]");
            sb.append(": (").append(realCause.getClass().getSimpleName()).append(") ").append(realCause.getMessage());
            return sb;
        });
    }

    @Override
    protected Observable<Object> afterObservable(Method method, Observable<Object> result, Long startTime) {
        long methodExitTime = clock.wallTime();

        AtomicInteger subscriptionCount = new AtomicInteger();
        return Observable.unsafeCreate(subscriber -> {
            long start = clock.wallTime();
            int idx = subscriptionCount.incrementAndGet();

            logOnSubscribe(method, methodExitTime, start, idx);

            Queue<Object> emittedItems = new ConcurrentLinkedQueue<>();
            AtomicInteger emittedCounter = new AtomicInteger();
            result.subscribe(
                    next -> {
                        if (emittedCounter.getAndIncrement() < MAX_EMITTED_ITEMS_BUFFER) {
                            emittedItems.add(next);
                        }
                        subscriber.onNext(next);
                    },
                    cause -> {
                        logWithPriority(observableErrorLevel, () -> {
                            Throwable realCause = cause instanceof InvocationTargetException ? cause.getCause() : cause;

                            StringBuilder sb = new StringBuilder("Error in subscription #").append(idx);
                            sb.append(" in ").append(getMethodSignature(method));
                            sb.append(" with ").append(realCause.getClass().getSimpleName()).append(" (")
                                    .append(realCause.getMessage()).append(')');
                            sb.append("; emitted ").append(emittedCounter.get()).append(" items ").append(emittedItems);
                            if (emittedCounter.get() > emittedItems.size()) {
                                sb.append("...");
                            }
                            return sb;
                        });
                        subscriber.onError(cause);
                    },
                    () -> {
                        logWithPriority(observableReplyLevel, () -> {
                            StringBuilder sb = new StringBuilder("Completed subscription #").append(idx);
                            sb.append(" in ").append(getMethodSignature(method));
                            sb.append("; emitted ").append(emittedCounter.get()).append(" items ").append(emittedItems);
                            if (emittedCounter.get() > emittedItems.size()) {
                                sb.append("...");
                            }
                            return sb;
                        });
                        subscriber.onCompleted();
                    }
            );
        });
    }

    @Override
    protected Completable afterCompletable(Method method, Completable result, Long aLong) {
        long methodExitTime = clock.wallTime();

        AtomicInteger subscriptionCount = new AtomicInteger();
        return Completable.create(subscriber -> {
            long start = clock.wallTime();
            int idx = subscriptionCount.incrementAndGet();

            logOnSubscribe(method, methodExitTime, start, idx);

            result.subscribe(
                    () -> {
                        logWithPriority(observableReplyLevel, () -> {
                            StringBuilder sb = new StringBuilder("Completed subscription #").append(idx);
                            sb.append(" in ").append(getMethodSignature(method));
                            return sb;
                        });
                        subscriber.onCompleted();
                    },
                    cause -> {
                        logWithPriority(observableErrorLevel, () -> {
                            Throwable realCause = cause instanceof InvocationTargetException ? cause.getCause() : cause;

                            StringBuilder sb = new StringBuilder("Error in subscription #").append(idx);
                            sb.append(" in ").append(getMethodSignature(method));
                            sb.append(" with ").append(realCause.getClass().getSimpleName()).append(" (")
                                    .append(realCause.getMessage()).append(')');
                            return sb;
                        });
                        subscriber.onError(cause);
                    }
            );
        });
    }

    private void logOnSubscribe(Method method, long methodExitTime, long start, int idx) {
        logWithPriority(observableReplyLevel, () -> {
            StringBuilder sb = new StringBuilder("Subscription #").append(idx);
            sb.append(" in ").append(getMethodSignature(method));
            sb.append(" after ").append(start - methodExitTime).append("[ms]");
            return sb;
        });
    }

    private void logWithPriority(LoggingProxyBuilder.Priority priority, Supplier<StringBuilder> logBuilder) {
        switch (priority) {
            case ERROR:
                if (logger.isErrorEnabled()) {
                    logger.error(trimToMaxContentLength(logBuilder.get()));
                }
                break;
            case INFO:
                if (logger.isInfoEnabled()) {
                    logger.info(trimToMaxContentLength(logBuilder.get()));
                }
                break;
            case DEBUG:
                if (logger.isDebugEnabled()) {
                    logger.debug(trimToMaxContentLength(logBuilder.get()));
                }
                break;
            case NEVER:
                // Do Nothing
        }
    }

    private String trimToMaxContentLength(StringBuilder sb) {
        if (sb.length() > MAX_CONTENT_LENGTH) {
            sb.setLength(MAX_CONTENT_LENGTH);
            sb.append("...");
        }
        return sb.toString();
    }
}
