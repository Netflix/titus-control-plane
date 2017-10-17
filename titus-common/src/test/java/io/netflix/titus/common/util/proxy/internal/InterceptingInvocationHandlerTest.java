/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.util.proxy.internal;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import io.netflix.titus.common.util.proxy.MyApi;
import org.junit.Test;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class InterceptingInvocationHandlerTest {

    private enum InterceptionPoint {Before, After, AfterException, AfterObservable}

    private static final String MESSAGE = "abcdefg";

    private final TestableInterceptingInvocationHandler handler = new TestableInterceptingInvocationHandler();

    private final MyApi myApi = (MyApi) Proxy.newProxyInstance(
            MyApi.class.getClassLoader(),
            new Class<?>[]{MyApi.class},
            new InvocationHandlerBridge<>(handler, (MyApi) new MyApi.MyApiImpl())
    );

    @Test
    public void testSuccessfulMethodInvocation() throws Exception {
        assertThat(myApi.echo(MESSAGE)).startsWith(MESSAGE);
        verifyIntereceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After);
    }

    @Test
    public void testFailedMethodInvocation() throws Exception {
        try {
            myApi.echo(null);
            fail("Exception expected");
        } catch (NullPointerException e) {
            verifyIntereceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.AfterException);
        }
    }

    @Test
    public void testObservableResult() throws Exception {
        assertThat(myApi.observableEcho(MESSAGE).toBlocking().first()).startsWith(MESSAGE);
        verifyIntereceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After, InterceptionPoint.AfterObservable);
    }

    @Test
    public void testFailedObservableResult() throws Exception {
        try {
            myApi.observableEcho(null).toBlocking().first();
            fail("Exception expected");
        } catch (NullPointerException e) {
            verifyIntereceptionPointsCalled(InterceptionPoint.Before, InterceptionPoint.After, InterceptionPoint.AfterObservable);
        }
    }

    private void verifyIntereceptionPointsCalled(InterceptionPoint... interceptionPoints) {
        assertThat(handler.interceptionPoints).hasSize(interceptionPoints.length);
        for (int i = 0; i < interceptionPoints.length; i++) {
            assertThat(interceptionPoints[i]).isEqualTo(handler.interceptionPoints.get(i));
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
    }
}