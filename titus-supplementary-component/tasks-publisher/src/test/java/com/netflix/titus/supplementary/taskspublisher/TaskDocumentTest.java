package com.netflix.titus.supplementary.taskspublisher;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TaskDocumentTest {

    @Test
    public void sanitizeEnvMap() {
        Map<String, String> env = new HashMap<>();
        String key1 = ".FOO";
        String key2 = "foo.bar";
        String key3 = "foo..bar";
        String key4 = "platform";
        String key5 = "foo.bar.";
        String key6 = "foo.bar...more";
        env.put(key1, "bar");
        env.put(key2, "ok");
        env.put(key3, "notOk");
        env.put(key4, "titus");
        env.put(key5, "notGood");
        env.put(key6, "bad");

        Map<String, String> sanitizeEnvMap = TaskDocument.sanitizeEnvMap(env);
        assertThat(sanitizeEnvMap).isNotNull();
        assertThat(sanitizeEnvMap.size()).isEqualTo(2);
        assertThat(sanitizeEnvMap.containsKey(key1)).isFalse();
        assertThat(sanitizeEnvMap.containsKey(key2)).isTrue();
        assertThat(sanitizeEnvMap.containsKey(key3)).isFalse();
        assertThat(sanitizeEnvMap.containsKey(key4)).isTrue();
        assertThat(sanitizeEnvMap.containsKey(key5)).isFalse();
        assertThat(sanitizeEnvMap.containsKey(key6)).isFalse();
    }

    @Test
    public void emptyEnvMap() {
        Map<String, String> sanitizeEnvMap = TaskDocument.sanitizeEnvMap(null);
        assertThat(sanitizeEnvMap).isNotNull();
        assertThat(sanitizeEnvMap.size()).isEqualTo(0);
    }

}