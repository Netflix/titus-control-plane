package io.netflix.titus.common.util.histogram;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HistogramTest {

    @Test
    public void testCorrectness() throws Exception {
        Histogram.Builder builder = Histogram.newBuilder(HistogramDescriptor.histogramOf(2, 5, 30, 60));
        for (int i = 0; i <= 100; i++) {
            builder.increment(i);
        }
        Histogram histogram = builder.build();
        assertThat(histogram.getCounters()).containsExactlyInAnyOrder(3L, 3L, 25L, 30L, 40L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongBounds() throws Exception {
        HistogramDescriptor.histogramOf(1, 2, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoBounds() throws Exception {
        HistogramDescriptor.histogramOf();
    }
}