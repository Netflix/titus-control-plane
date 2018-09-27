package com.netflix.titus.common.util;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static com.netflix.titus.common.util.CollectionsExt.xor;
import static org.assertj.core.api.Assertions.assertThat;

public class CollectionsExtTest {

    @Test
    public void testSetXor() {
        assertThat(xor(asSet(1, 2, 3), asSet(2, 3, 4), asSet(3, 4, 5))).contains(1, 5);
    }

    @Test
    public void testContainsAnyKeys() {
        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        assertThat(CollectionsExt.containsAnyKeys(map, "a", "c")).isTrue();
        assertThat(CollectionsExt.containsAnyKeys(map, "c", "a")).isTrue();
        assertThat(CollectionsExt.containsAnyKeys(map, "c", "d")).isFalse();
    }
}