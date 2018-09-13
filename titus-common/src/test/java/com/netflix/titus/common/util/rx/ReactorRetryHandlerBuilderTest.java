package com.netflix.titus.common.util.rx;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static com.netflix.titus.common.util.rx.RetryHandlerBuilder.retryHandler;

public class ReactorRetryHandlerBuilderTest {

    private static final long RETRY_DELAY_SEC = 1;

    @Test
    public void testRetryOnError() {
        StepVerifier
                .withVirtualTime(() ->
                        streamOf("A", new IOException("Error1"), "B", new IOException("Error2"), "C")
                                .retryWhen(newRetryHandlerBuilder().buildReactorExponentialBackoff())
                )
                // Expect first item
                .expectNext("A")

                // Expect second item
                .expectNoEvent(Duration.ofSeconds(RETRY_DELAY_SEC))
                .expectNext("B")

                // Expect third item
                .expectNoEvent(Duration.ofSeconds(2 * RETRY_DELAY_SEC))
                .expectNext("C")

                .verifyComplete();
    }

    @Test
    public void testMaxRetryDelay() {
        long expectedDelay = RETRY_DELAY_SEC + 2 * RETRY_DELAY_SEC + 2 * RETRY_DELAY_SEC;

        StepVerifier
                .withVirtualTime(() ->
                        streamOf(new IOException("Error1"), new IOException("Error2"), new IOException("Error3"), "A")
                                .retryWhen(newRetryHandlerBuilder()
                                        .withMaxDelay(RETRY_DELAY_SEC * 2, TimeUnit.SECONDS)
                                        .buildReactorExponentialBackoff()
                                )
                )
                .expectSubscription()

                // Expect first item
                .expectNoEvent(Duration.ofSeconds(expectedDelay))
                .expectNext("A")

                .verifyComplete();
    }

    @Test
    public void testMaxRetry() {
        StepVerifier
                .withVirtualTime(() ->
                        streamOf(new IOException("Error1"), new IOException("Error2"), "A")
                                .retryWhen(newRetryHandlerBuilder()
                                        .withRetryCount(1)
                                        .buildReactorExponentialBackoff()
                                )
                )
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(RETRY_DELAY_SEC))
                .verifyError(IOException.class);
    }

    private Flux<String> streamOf(Object... items) {
        AtomicInteger pos = new AtomicInteger();
        return Flux.create(emitter -> {
            for (int i = pos.get(); i < items.length; i++) {
                pos.incrementAndGet();
                Object item = items[i];

                if (item instanceof Throwable) {
                    emitter.error((Throwable) item);
                    return;
                }
                emitter.next((String) item);
            }
            emitter.complete();
        });
    }

    private RetryHandlerBuilder newRetryHandlerBuilder() {
        return retryHandler()
                .withReactorScheduler(Schedulers.parallel())
                .withTitle("testObservable")
                .withRetryCount(3)
                .withRetryDelay(RETRY_DELAY_SEC, TimeUnit.SECONDS);
    }
}
