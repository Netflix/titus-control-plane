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

package com.netflix.titus.common.util.rx;

import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;

public class InContextOnSubscribeTest {

    @Test
    public void testContextCreation() throws Exception {
        ExtTestSubscriber<String> testSubscriber = new ExtTestSubscriber<>();
        ObservableExt.inContext(() -> "MyContext", Observable::just).subscribe(testSubscriber);

        assertThat(testSubscriber.takeNext()).isEqualTo("MyContext");
        assertThat(testSubscriber.isUnsubscribed()).isTrue();
    }
}