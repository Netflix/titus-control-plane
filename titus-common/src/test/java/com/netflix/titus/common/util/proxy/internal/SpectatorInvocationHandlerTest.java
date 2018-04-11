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

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.proxy.MyApi;
import com.netflix.titus.common.util.proxy.ProxyCatalog;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SpectatorInvocationHandlerTest {

    private static final String MESSAGE = "abcdefg";

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final MyApi myApi = ProxyCatalog.createSpectatorProxy(MyApi.class, new MyApi.MyApiImpl(), titusRuntime);

    @Test
    public void testSuccessfulMethodInvocation() {
        assertThat(myApi.echo("abc")).startsWith("abc");
    }

    @Test(expected = NullPointerException.class)
    public void testFailedMethodInvocation() {
        myApi.echo(null);
    }

    @Test
    public void testObservableResult() {
        assertThat(myApi.observableEcho(MESSAGE).toBlocking().first()).startsWith(MESSAGE);
    }

    @Test(expected = NullPointerException.class)
    public void testFailedObservableResult() {
        assertThat(myApi.observableEcho(null).toBlocking().first());
    }

    @Test
    public void testCompletable() {
        assertThat(myApi.okCompletable().get()).isNull();
    }

    @Test
    public void testFailingCompletable() {
        assertThat(myApi.failingCompletable().get()).isInstanceOf(RuntimeException.class);
    }
}