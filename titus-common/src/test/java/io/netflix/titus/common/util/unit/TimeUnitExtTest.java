package io.netflix.titus.common.util.unit;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeUnitExtTest {

    @Test
    public void testParse() {
        testParse("1ms", 1, TimeUnit.MILLISECONDS);
        testParse("2s", 2, TimeUnit.SECONDS);
        testParse("3m", 3, TimeUnit.MINUTES);
        testParse("4h", 4, TimeUnit.HOURS);
        testParse("5d", 5, TimeUnit.DAYS);
    }

    @Test
    public void testParseWithInvalidData() {
        assertThat(TimeUnitExt.parse("1b2mss")).isNotPresent();
    }

    @Test
    public void testToMillis() {
        assertThat(TimeUnitExt.toMillis("1m").get()).isEqualTo(60_000);
    }

    private void testParse(String value, long expectedInterval, TimeUnit expectedUnit) {
        Optional<Pair<Long, TimeUnit>> result = TimeUnitExt.parse(value);
        assertThat(result).isPresent();
        assertThat(result.get().getLeft()).isEqualTo(expectedInterval);
        assertThat(result.get().getRight()).isEqualTo(expectedUnit);
    }
}