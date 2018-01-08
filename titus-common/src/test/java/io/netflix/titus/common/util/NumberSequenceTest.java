package io.netflix.titus.common.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import io.netflix.titus.common.util.tuple.Either;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class NumberSequenceTest {

    @Test
    public void testParse() {
        checkSequence(tryParse(""), Collections.emptyList(), false);
        checkSequence(tryParse("1,2,3"), asList(1L, 2L, 3L), false);
        checkSequence(tryParse("1..5"), asList(1L, 2L, 3L, 4L), false);
        checkSequence(tryParse("1.."), asList(1L, 2L, 3L, 4L, 5L, 6L), true);
        checkSequence(tryParse("1, 2, 3, 4..6, 10"), asList(1L, 2L, 3L, 4L, 5L, 10L), false);
    }

    @Test
    public void testSkip() {
        checkSequence(tryParse("1,2,3,4,5").skip(2), asList(3L, 4L, 5L), false);
        checkSequence(tryParse("1..10").skip(5), asList(6L, 7L, 8L, 9L), false);
        checkSequence(tryParse("1,2,3,8..10,15,16").skip(5), asList(15L, 16L), false);
        checkSequence(tryParse("1,2,3,8..10,15,16").skip(1000), Collections.emptyList(), false);
    }

    @Test
    public void testStep() {
        checkSequence(tryParse("1,2,3,4,5").step(2), asList(1L, 3L, 5L), false);
        checkSequence(tryParse("1..5").step(2), asList(1L, 3L), false);
        checkSequence(tryParse("1,2,3,4..7,12..14").step(2), asList(1L, 3L, 5L, 12L), false);
        checkSequence(tryParse("1,2,3,4..7,12..14").skip(2).step(2), asList(3L, 5L, 12L), false);
    }

    private NumberSequence tryParse(String value) {
        Either<NumberSequence, String> result = NumberSequence.parse(value);
        if (result.hasError()) {
            fail(result.getError());
        }
        return result.getValue();
    }

    private void checkSequence(NumberSequence sequence, List<Long> expected, boolean infinite) {
        Iterator<Long> it = sequence.getIterator();
        for (long nextExpected : expected) {
            assertThat(it.next()).isEqualTo(nextExpected);
        }
        if (infinite) {
            assertThat(it.hasNext()).isTrue();
        } else {
            assertThat(it.hasNext()).isFalse();
        }
    }
}