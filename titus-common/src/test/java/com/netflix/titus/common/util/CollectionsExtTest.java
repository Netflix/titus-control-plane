package com.netflix.titus.common.util;

import org.junit.Test;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static com.netflix.titus.common.util.CollectionsExt.xor;
import static org.assertj.core.api.Assertions.assertThat;

public class CollectionsExtTest {

    @Test
    public void testSetXor() {
        assertThat(xor(asSet(1, 2, 3), asSet(2, 3, 4), asSet(3, 4, 5))).contains(1, 5);
    }
}