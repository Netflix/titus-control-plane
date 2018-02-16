package io.netflix.titus.common.util;

import java.util.regex.Pattern;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RegExpExtTest {

    @Test
    public void testDynamicMatcher() {
        String message = "hello\nend";
        boolean matches = RegExpExt.dynamicMatcher(() -> ".*hello.*", Pattern.DOTALL, e -> {
            throw new IllegalStateException(e);
        }).apply(message).matches();
        assertThat(matches).isTrue();
    }
}