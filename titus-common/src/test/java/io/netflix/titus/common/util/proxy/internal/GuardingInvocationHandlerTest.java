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

import io.netflix.titus.common.util.proxy.MyApi;
import io.netflix.titus.common.util.proxy.ProxyCatalog;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GuardingInvocationHandlerTest {

    private static final String MESSAGE = "abcdefg";

    private boolean flag;

    private final MyApi myApi = ProxyCatalog.createGuardingProxy(MyApi.class, new MyApi.MyApiImpl(), () -> flag);

    @Test(expected = IllegalStateException.class)
    public void testCallsToMethodsAreGuarded() throws Exception {
        myApi.echo(MESSAGE);
    }

    @Test
    public void testCallsToMethodsArePassedThrough() throws Exception {
        flag = true;
        assertThat(myApi.echo(MESSAGE)).startsWith(MESSAGE);
    }
}