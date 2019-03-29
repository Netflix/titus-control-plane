/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.common.util.rx;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class EventEmitterTransformerTest {

    private static final StringEvent SNAPSHOT_END = new StringEvent("SNAPSHOT_END", true);

    private final DirectProcessor<List<String>> source = DirectProcessor.create();
    private final Flux<StringEvent> events = source.compose(ReactorExt.eventEmitter(
            string -> string.substring(0, 1),
            String::equals,
            string -> new StringEvent(string, true),
            string -> new StringEvent(string, false),
            SNAPSHOT_END
    ));

    @Test
    public void testAdd() {
        Iterator<StringEvent> output = events.toIterable().iterator();

        source.onNext(asList("a", "b"));
        assertThat(output.next()).isEqualTo(new StringEvent("a", true));
        assertThat(output.next()).isEqualTo(new StringEvent("b", true));
        assertThat(output.next()).isEqualTo(SNAPSHOT_END);

        source.onNext(asList("a", "b", "c"));
        assertThat(output.next()).isEqualTo(new StringEvent("c", true));
    }

    @Test
    public void testRemove() {
        Iterator<StringEvent> output = events.toIterable().iterator();

        source.onNext(asList("a", "b"));
        assertThat(output.next()).isEqualTo(new StringEvent("a", true));
        assertThat(output.next()).isEqualTo(new StringEvent("b", true));
        assertThat(output.next()).isEqualTo(SNAPSHOT_END);

        source.onNext(asList("b", "c"));
        assertThat(output.next()).isEqualTo(new StringEvent("c", true));
        assertThat(output.next()).isEqualTo(new StringEvent("a", false));
    }

    @Test
    public void testUpdate() {
        Iterator<StringEvent> output = events.toIterable().iterator();

        source.onNext(asList("a", "b"));
        assertThat(output.next()).isEqualTo(new StringEvent("a", true));
        assertThat(output.next()).isEqualTo(new StringEvent("b", true));
        assertThat(output.next()).isEqualTo(SNAPSHOT_END);

        source.onNext(asList("av2"));
        assertThat(output.next()).isEqualTo(new StringEvent("av2", true));
        assertThat(output.next()).isEqualTo(new StringEvent("b", false));
    }

    private static class StringEvent {

        private final String value;
        private final boolean added;

        private StringEvent(String value, boolean added) {
            this.value = value;
            this.added = added;
        }

        public String getValue() {
            return value;
        }

        public boolean isAdded() {
            return added;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StringEvent that = (StringEvent) o;
            return added == that.added &&
                    Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, added);
        }

        @Override
        public String toString() {
            return "StringEvent{" +
                    "value='" + value + '\'' +
                    ", added=" + added +
                    '}';
        }
    }
}