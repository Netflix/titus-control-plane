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

package io.netflix.titus.ext.aws;

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import org.junit.Test;
import rx.Completable;
import rx.Single;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.junit.Assert.assertEquals;

public class AwsObservableExtTest {
    @Test
    public void asyncActionCompletable() throws Exception {
        AmazonWebServiceRequest someRequest = AmazonWebServiceRequest.NOOP;
        final MockAsyncClient<AmazonWebServiceRequest, String> client = new MockAsyncClient<>(someRequest, "some response");
        final Completable completable = AwsObservableExt.asyncActionCompletable(factory -> client.someAsyncOperation(factory.handler(
                (req, resp) -> {
                    assertEquals(someRequest, req);
                    assertEquals("some response", resp);
                },
                (t) -> {
                    throw new IllegalStateException("Should never be here");
                }
        )));

        TestScheduler testScheduler = Schedulers.test();
        final AssertableSubscriber<Void> subscriber = completable.subscribeOn(testScheduler).test();

        testScheduler.triggerActions();
        subscriber.assertNotCompleted();

        client.run();
        testScheduler.triggerActions();
        subscriber.assertCompleted();
    }

    @Test
    public void asyncActionSingle() throws Exception {
        AmazonWebServiceRequest someRequest = AmazonWebServiceRequest.NOOP;
        final MockAsyncClient<AmazonWebServiceRequest, String> client = new MockAsyncClient<>(someRequest, "some response");
        Single<String> single = AwsObservableExt.asyncActionSingle(supplier -> client.someAsyncOperation(supplier.handler()));

        TestScheduler testScheduler = Schedulers.test();
        final AssertableSubscriber<String> subscriber = single.subscribeOn(testScheduler).test();

        testScheduler.triggerActions();
        subscriber.assertNoValues();
        subscriber.assertNotCompleted();

        client.run();
        testScheduler.triggerActions();
        subscriber.assertValueCount(1);
        subscriber.assertValue("some response");
        subscriber.assertCompleted();
    }

    private class MockAsyncClient<REQ extends AmazonWebServiceRequest, RES> {
        private final REQ request;
        private final RES response;

        private volatile FutureTask<RES> futureTask;

        private MockAsyncClient(REQ request, RES response) {
            this.request = request;
            this.response = response;
        }

        Future<RES> someAsyncOperation(AsyncHandler<? super REQ, RES> handler) {
            futureTask = new FutureTask<RES>(() -> {
                handler.onSuccess(request, response);
                return response;
            });
            return futureTask;
        }

        private void run() {
            futureTask.run();
        }
    }

}
