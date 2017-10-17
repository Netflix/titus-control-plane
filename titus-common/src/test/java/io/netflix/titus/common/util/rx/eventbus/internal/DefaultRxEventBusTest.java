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

package io.netflix.titus.common.util.rx.eventbus.internal;

import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.testkit.junit.resource.Log4jExternalResource;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import rx.Subscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultRxEventBusTest {

    private static final long MAX_QUEUE_SIZE = 1;

    @Rule
    public final Log4jExternalResource loggingActivator = Log4jExternalResource.enableFor(DefaultRxEventBus.class);

    private final TestScheduler testScheduler = Schedulers.test();

    private final Registry registry = new DefaultRegistry();

    private final RxEventBus eventBus = new DefaultRxEventBus(registry.createId("test"), registry, MAX_QUEUE_SIZE, testScheduler);

    @After
    public void tearDown() throws Exception {
        eventBus.close();
    }

    @Test
    public void testDirectEventPublishing() throws Exception {
        ExtTestSubscriber<String> testSubscriber = new ExtTestSubscriber<>();
        eventBus.listen("myClient", String.class).subscribe(testSubscriber);

        eventBus.publish("event1");

        assertThat(testSubscriber.takeNext()).isEqualTo("event1");
        assertThat(testSubscriber.takeNext()).isNull();
    }

    @Test
    public void testAsyncEventPublishing() throws Exception {
        ExtTestSubscriber<String> testSubscriber = new ExtTestSubscriber<>();
        eventBus.listen("myClient", String.class).subscribe(testSubscriber);

        assertThat(testSubscriber.takeNext()).isNull();
        eventBus.publishAsync("event1");
        testScheduler.triggerActions();

        assertThat(testSubscriber.takeNext()).isEqualTo("event1");
        assertThat(testSubscriber.takeNext()).isNull();
    }

    @Test
    public void testEventBusCloseTerminatesSubscriptions() throws Exception {
        ExtTestSubscriber<String> testSubscriber = new ExtTestSubscriber<>();
        eventBus.listen("myClient", String.class).subscribe(testSubscriber);

        eventBus.close();
        testSubscriber.assertOnCompleted();
    }

    @Test
    public void testSlowConsumerTerminatesWithOverflowError() throws Exception {
        AtomicBoolean failed = new AtomicBoolean();
        Subscriber<String> slowSubscriber = new Subscriber<String>() {
            @Override
            public void onStart() {
                request(0);
            }

            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
                failed.set(true);
            }

            @Override
            public void onNext(String s) {
            }
        };

        eventBus.listen("myClient", String.class).subscribe(slowSubscriber);
        for (int i = 0; i <= MAX_QUEUE_SIZE; i++) {
            eventBus.publish("event" + i);
        }

        assertThat(failed.get()).isTrue();
        assertThat(slowSubscriber.isUnsubscribed()).isTrue();
    }
}