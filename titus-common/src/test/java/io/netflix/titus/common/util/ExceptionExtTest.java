package io.netflix.titus.common.util;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ExceptionExtTest {

    @Test
    public void testToMessageChain() throws Exception {
        String messageChain = ExceptionExt.toMessageChain(new RuntimeException("outside", new Exception("inside")));
        assertThat(messageChain).contains("(RuntimeException) outside -CAUSED BY-> (Exception) inside");
    }
}