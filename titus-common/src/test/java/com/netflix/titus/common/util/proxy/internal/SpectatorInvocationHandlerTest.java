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

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.proxy.MyApi;
import com.netflix.titus.common.util.proxy.MyApi;
import com.netflix.titus.common.util.proxy.ProxyCatalog;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SpectatorInvocationHandlerTest {

    private static final String MESSAGE = "abcdefg";

    private final Registry registry = new DefaultRegistry();

    private final MyApi myApi = ProxyCatalog.createSpectatorProxy(MyApi.class, new MyApi.MyApiImpl(), registry);

    @Test
    public void testSuccessfulMethodInvocation() throws Exception {
        assertThat(myApi.echo("abc")).startsWith("abc");
    }

    @Test(expected = NullPointerException.class)
    public void testFailedMethodInvocation() throws Exception {
        myApi.echo(null);
    }

    @Test
    public void testObservableResult() throws Exception {
        assertThat(myApi.observableEcho(MESSAGE).toBlocking().first()).startsWith(MESSAGE);
    }

    @Test(expected = NullPointerException.class)
    public void testFailedObservableResult() throws Exception {
        assertThat(myApi.observableEcho(null).toBlocking().first());
    }
}