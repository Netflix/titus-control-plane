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

package com.netflix.titus.common.framework.fit.internal;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.framework.fit.internal.action.FitErrorAction;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class FitInvocationHandlerTest {

    private final FitFramework fitFramework = FitFramework.newFitFramework();

    private final FitInjection fitInjection = fitFramework.newFitInjectionBuilder("testInjection")
            .withExceptionType(MyException.class)
            .build();

    private final MyApiImpl myApiImpl = new MyApiImpl();

    private final MyApi myApi = fitFramework.newFitProxy(myApiImpl, fitInjection);

    @Before
    public void setUp() {
        fitFramework.getRootComponent().addInjection(fitInjection);
    }

    @Test
    public void testBeforeSynchronous() {
        configureAction(true);
        try {
            myApi.runSynchronous("hello");
            fail("Expected FIT injected error");
        } catch (MyException e) {
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(0);
        }

        assertThat(myApi.runSynchronous("hello")).isEqualTo("hello");
    }

    @Test
    public void testAfterSynchronous() {
        configureAction(false);
        try {
            myApi.runSynchronous("hello");
            fail("Expected FIT injected error");
        } catch (MyException e) {
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(1);
        }

        assertThat(myApi.runSynchronous("hello")).isEqualTo("hello");
    }

    @Test
    public void testBeforeCompletableFuture() throws Exception {
        runBefore(true, () -> myApi.runCompletableFuture("hello").get(), 0);
    }

    @Test
    public void testAfterCompletableFuture() throws Exception {
        runBefore(false, () -> myApi.runCompletableFuture("hello").get(), 1);
    }

    @Test
    public void testBeforeListenableFuture() throws Exception {
        runBefore(true, () -> myApi.runListenableFuture("hello").get(), 0);
    }

    @Test
    public void testAfterListenableFuture() throws Exception {
        runBefore(false, () -> myApi.runListenableFuture("hello").get(), 1);
    }

    private void runBefore(boolean before, Callable<String> action, int executionCount) throws Exception {
        configureAction(before);
        try {
            action.call();
            fail("Expected FIT injected error");
        } catch (Exception e) {
            assertThat(e.getCause()).isInstanceOf(MyException.class);
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(executionCount);
        }
        assertThat(action.call()).isEqualTo("hello");
    }

    @Test
    public void testBeforeObservable() {
        configureAction(true);
        try {
            myApi.runObservable("hello").toBlocking().last();
            fail("Expected FIT injected error");
        } catch (MyException e) {
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(0);
        }

        assertThat(myApi.runObservable("hello").toBlocking().first()).isEqualTo("hello");
    }

    @Test
    public void testAfterObservable() {
        configureAction(false);
        try {
            myApi.runObservable("hello").toBlocking().last();
            fail("Expected FIT injected error");
        } catch (MyException e) {
            assertThat(myApiImpl.getExecutionCounter()).isEqualTo(1);
        }

        assertThat(myApi.runObservable("hello").toBlocking().first()).isEqualTo("hello");
    }

    private void configureAction(boolean before) {
        fitInjection.addAction(new FitErrorAction("errorAction", ImmutableMap.of(
                "percentage", "20",
                "before", Boolean.toString(before),
                "errorTime", "1m",
                "upTime", "0s"
        ), fitInjection));
    }

    public static class MyException extends RuntimeException {
        public MyException(String message) {
            super(message);
        }
    }

    public interface MyApi {
        String runSynchronous(String hello);

        CompletableFuture<String> runCompletableFuture(String arg);

        ListenableFuture<String> runListenableFuture(String arg);

        Observable<String> runObservable(String arg);
    }

    public static class MyApiImpl implements MyApi {

        private volatile int executionCounter;

        public int getExecutionCounter() {
            return executionCounter;
        }

        @Override
        public String runSynchronous(String arg) {
            executionCounter++;
            return arg;
        }

        @Override
        public ListenableFuture<String> runListenableFuture(String arg) {
            executionCounter++;
            return Futures.immediateFuture(arg);
        }

        @Override
        public CompletableFuture<String> runCompletableFuture(String arg) {
            executionCounter++;
            return CompletableFuture.completedFuture(arg);
        }

        @Override
        public Observable<String> runObservable(String arg) {
            return Observable.fromCallable(() -> {
                executionCounter++;
                return arg;
            });
        }
    }
}