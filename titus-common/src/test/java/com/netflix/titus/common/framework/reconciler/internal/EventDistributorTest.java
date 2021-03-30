/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.framework.reconciler.internal;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import org.junit.Test;
import rx.Emitter;
import rx.Observable;
import rx.Subscription;

import static org.assertj.core.api.Assertions.assertThat;

public class EventDistributorTest {

    private static final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final EventDistributor<SimpleReconcilerEvent> eventDistributor = new EventDistributor<>(titusRuntime.getRegistry());

    @Test
    public void testBootstrap() throws Exception {
        EngineStub engine1 = new EngineStub("engine1");
        EngineStub engine2 = new EngineStub("engine2");
        eventDistributor.connectReconciliationEngine(engine1);
        eventDistributor.connectReconciliationEngine(engine2);
        eventDistributor.start();

        MyClient client = new MyClient();
        eventDistributor.connectEmitter(client.getEmitter());

        engine1.emitChange("engine1/change1");
        engine2.emitChange("engine2/change1", "engine2/change2");
        engine1.emitChange("engine1/change2");
        client.expectChanges("engine1/change1", "engine2/change1", "engine2/change2", "engine1/change2");
    }

    @Test
    public void testEngineAddRemove() throws Exception {
        eventDistributor.start();

        EngineStub engine1 = new EngineStub("engine1");
        EngineStub engine2 = new EngineStub("engine2");
        eventDistributor.connectReconciliationEngine(engine1);
        eventDistributor.connectReconciliationEngine(engine2);

        MyClient client = new MyClient();
        eventDistributor.connectEmitter(client.getEmitter());

        engine1.emitChange("engine1/change1");
        engine2.emitChange("engine2/change1");
        client.expectChanges("engine1/change1", "engine2/change1");

        eventDistributor.removeReconciliationEngine(engine1);
        engine1.emitChange("engine1/change2");
        engine2.emitChange("engine2/change2");
        client.expectChanges("engine2/change2");
    }

    @Test
    public void testClientAddUnsubscribe() throws Exception {
        eventDistributor.start();

        EngineStub engine1 = new EngineStub("engine1");
        eventDistributor.connectReconciliationEngine(engine1);

        MyClient client1 = new MyClient();
        eventDistributor.connectEmitter(client1.getEmitter());
        MyClient client2 = new MyClient();
        eventDistributor.connectEmitter(client2.getEmitter());

        engine1.emitChange("engine1/change1");
        client1.expectChanges("engine1/change1");
        client2.expectChanges("engine1/change1");

        client1.unsubscribe();

        engine1.emitChange("engine1/change2");
        client2.expectChanges("engine1/change2");
        client1.expectNoEvents();
    }

    @Test
    public void testShutdown() throws Exception {
        eventDistributor.start();

        EngineStub engine1 = new EngineStub("engine1");
        eventDistributor.connectReconciliationEngine(engine1);

        MyClient client1 = new MyClient();
        eventDistributor.connectEmitter(client1.getEmitter());

        engine1.emitChange("engine1/change1");
        client1.expectChanges("engine1/change1");

        eventDistributor.stop(30_000);

        client1.expectStopped();
    }

    static class MyClient {

        private Emitter<SimpleReconcilerEvent> emitter;

        private final LinkedBlockingQueue<SimpleReconcilerEvent> receivedEvents = new LinkedBlockingQueue<>();
        private final Subscription subscription;

        private volatile Throwable error;
        private volatile boolean completed;

        MyClient() {
            Observable<SimpleReconcilerEvent> observable = Observable.create(emitter -> {
                MyClient.this.emitter = emitter;
            }, Emitter.BackpressureMode.ERROR);
            this.subscription = observable
                    .subscribe(
                            receivedEvents::add,
                            e -> error = e,
                            () -> completed = true
                    );
        }

        Emitter<SimpleReconcilerEvent> getEmitter() {
            return emitter;
        }

        void expectChanges(String... changes) throws InterruptedException {
            for (String change : changes) {
                SimpleReconcilerEvent event = receivedEvents.poll(30, TimeUnit.SECONDS);
                if (event == null) {
                    throw new IllegalStateException("no event received in the configured time ");
                }
                assertThat(event.getMessage()).isEqualTo(change);
            }
        }

        void expectNoEvents() {
            assertThat(receivedEvents).isEmpty();
        }

        void unsubscribe() {
            subscription.unsubscribe();
        }

        void expectStopped() {
            assertThat(error).isNotNull();
            assertThat(error.getMessage()).isEqualTo("Reconciler framework stream closed");
        }
    }
}
