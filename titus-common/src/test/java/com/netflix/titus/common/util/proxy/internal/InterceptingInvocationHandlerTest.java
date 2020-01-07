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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import com.netflix.titus.common.util.proxy.MyApi;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class InterceptingInvocationHandlerTest {

    private enum InterceptionPoint {Before, After, AfterException, AfterObservable, AfterFlux, AfterCompletable, AfterMono}

    private static final String MESSAGE = "abcdefg";

    private final TestableInterceptingInvocationHandler handler = new TestableInterceptingInvocationHandler();

    private final MyApi myApi = (MyApi) Proxy.newProxyInstance(
            MyApi.class.getClassLoader(),
            new Class<?>[]{MyApi.class},
            new InvocationHandlerBridge<>(handler, (MyApi) new MyApi.MyApiImpl())
    );

    @Test
    public void testSuccessfulMethodInvocation() {
        assertThat(myApi.echo(MESSAGE)).startsWith(MESSAGE);
        verifyInterceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After);
    }

    @Test
    public void testFailedMethodInvocation() {
        try {
            myApi.echo(null);
            fail("Exception expected");
        } catch (NullPointerException e) {
            verifyInterceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.AfterException);
        }
    }

    @Test
    public void testObservableResult() {
        assertThat(myApi.observableEcho(MESSAGE).toBlocking().first()).startsWith(MESSAGE);
        verifyInterceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After, InterceptionPoint.AfterObservable);
    }

    @Test
    public void testFluxResult() {
        assertThat(myApi.fluxEcho(MESSAGE).blockFirst()).startsWith(MESSAGE);
        verifyInterceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After, InterceptionPoint.AfterFlux);
    }

    @Test
    public void testFailedObservableResult() {
        try {
            myApi.observableEcho(null).toBlocking().first();
            fail("Exception expected");
        } catch (NullPointerException e) {
            verifyInterceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After, InterceptionPoint.AfterObservable);
        }
    }

    @Test
    public void tesCompletableResult() {
        assertThat(myApi.okCompletable().get()).isNull();
        verifyInterceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After, InterceptionPoint.AfterCompletable);
    }

    @Test
    public void tesFailedCompletableResult() {
        assertThat(myApi.failingCompletable().get()).isInstanceOf(RuntimeException.class);
        verifyInterceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After, InterceptionPoint.AfterCompletable);
    }

    @Test
    public void testMonoResult() {
        assertThat(myApi.okMonoString().block()).isEqualTo("Hello");
        verifyInterceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After, InterceptionPoint.AfterMono);
    }

    @Test
    public void tesFailedMonoResult() {
        try {
            myApi.failingMonoString().block();
            fail("Mono failure expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(RuntimeException.class);
        }
        verifyInterceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After, InterceptionPoint.AfterMono);
    }

    private void verifyInterceptionPointsCalled(InterceptionPoint... interceptionPoints) {
        assertThat(handler.interceptionPoints).hasSize(interceptionPoints.length);
        for (int i = 0; i < interceptionPoints.length; i++) {
            assertThat(handler.interceptionPoints.get(i)).isEqualTo(interceptionPoints[i]);
        }
    }

    private static class TestableInterceptingInvocationHandler extends InterceptingInvocationHandler<MyApi, Object, Boolean> {

        List<InterceptionPoint> interceptionPoints = new ArrayList<>();

        TestableInterceptingInvocationHandler() {
            super(MyApi.class, false);
        }

        @Override
        protected Boolean before(Method method, Object[] args) {
            interceptionPoints.add(InterceptionPoint.Before);
            return true;
        }

        @Override
        protected void after(Method method, Object result, Boolean aBoolean) {
            interceptionPoints.add(InterceptionPoint.After);
        }

        @Override
        protected void afterException(Method method, Throwable cause, Boolean aBoolean) {
            interceptionPoints.add(InterceptionPoint.AfterException);
        }

        @Override
        protected Observable<Object> afterObservable(Method method, Observable<Object> result, Boolean aBoolean) {
            interceptionPoints.add(InterceptionPoint.AfterObservable);
            return result;
        }

        @Override
        protected Flux<Object> afterFlux(Method method, Flux<Object> result, Boolean aBoolean) {
            interceptionPoints.add(InterceptionPoint.AfterFlux);
            return result;
        }

        @Override
        protected Completable afterCompletable(Method method, Completable result, Boolean aBoolean) {
            interceptionPoints.add(InterceptionPoint.AfterCompletable);
            return result;
        }

        @Override
        protected Mono<Object> afterMono(Method method, Mono<Object> result, Boolean aBoolean) {
            interceptionPoints.add(InterceptionPoint.AfterMono);
            return result;
        }
    }
}