package com.netflix.titus.common.util.rx;

import java.util.Optional;

import org.junit.Test;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorExtTest {

    @Test
    public void testEmitValue() {
        Optional<Throwable> error = ReactorExt.emitError(Mono.just("Hello")).block();
        assertThat(error).isEmpty();
    }

    @Test
    public void testEmitError() {
        Optional<Throwable> error = ReactorExt.emitError(Mono.error(new RuntimeException("SimulatedError"))).single().block();
        assertThat(error).isPresent();
        assertThat(error.get()).isInstanceOf(RuntimeException.class);
    }
}