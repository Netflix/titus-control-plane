package com.netflix.titus.ext.jooqflyway.jobactivity;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.jooq.DSLContext;
import reactor.core.publisher.Mono;

public class JooqUtils {
    // Returns a CompletableStage that asynchronously runs the supplier on the DSL's executor
    public static <T> CompletionStage<T> executeAsync(Supplier<T> supplier, DSLContext dslContext) {
        return CompletableFuture.supplyAsync(supplier, dslContext.configuration().executorProvider().provide());
    }

    // Returns a Mono that asynchronously runs the supplier on the DSL's executor
    public static <T> Mono<T> executeAsyncMono(Supplier<T> supplier, DSLContext dslContext) {
        return Mono.fromCompletionStage(executeAsync(supplier, dslContext));
    }
}
