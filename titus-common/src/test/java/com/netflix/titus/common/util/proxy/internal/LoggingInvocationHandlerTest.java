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

import com.netflix.titus.common.util.proxy.LoggingProxyBuilder;
import com.netflix.titus.common.util.proxy.MyApi;
import com.netflix.titus.common.util.proxy.ProxyCatalog;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LoggingInvocationHandlerTest {

    private static final String MESSAGE = "abcdefg";

    private final MyApi myApi = ProxyCatalog.createLoggingProxy(MyApi.class, new MyApi.MyApiImpl())
            .request(LoggingProxyBuilder.Priority.INFO)
            .reply(LoggingProxyBuilder.Priority.INFO)
            .observableReply(LoggingProxyBuilder.Priority.INFO)
            .exception(LoggingProxyBuilder.Priority.INFO)
            .observableError(LoggingProxyBuilder.Priority.INFO)
            .build();

    @Test
    public void testSuccessfulMethodInvocationLogging() {
        assertThat(myApi.echo(MESSAGE)).startsWith(MESSAGE);
    }

    @Test(expected = NullPointerException.class)
    public void testFailedMethodInvocationLogging() {
        myApi.echo(null);
    }

    @Test
    public void testObservableResultLogging() {
        assertThat(myApi.observableEcho(MESSAGE).toBlocking().first()).startsWith(MESSAGE);
    }

    @Test(expected = NullPointerException.class)
    public void testFailedObservableResultLogging() {
        myApi.observableEcho(null).toBlocking().first();
    }

    @Test
    public void testCompletableResultLogging() {
        assertThat(myApi.okCompletable().get()).isNull();
    }

    @Test
    public void testFailingCompletableResultLogging() {
        assertThat(myApi.failingCompletable().get()).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testMonoString() {
        assertThat(myApi.okMonoString().block()).isEqualTo("Hello");
    }

    @Test(expected = RuntimeException.class)
    public void testFailingMonoString() {
        myApi.failingMonoString().block();
    }

    @Test
    public void testMonoVoid() {
        assertThat(myApi.okMonoVoid().block()).isNull();
    }
}