/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.framework.simplereconciler.internal;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.simplereconciler.SimpleReconcilerEvent;
import com.netflix.titus.common.util.closeable.CloseableReference;
import org.junit.After;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class ShardedManyReconcilerTest {

    private final StubManyReconciler<String> shard1 = new StubManyReconciler<>();
    private final StubManyReconciler<String> shard2 = new StubManyReconciler<>();

    private final CloseableReference<Scheduler> notificationSchedulerRef = CloseableReference.referenceOf(
            Schedulers.newSingle("reconciler-notification-junit", true), Scheduler::dispose
    );

    private final ShardedManyReconciler<String> reconciler = new ShardedManyReconciler<>(
            2,
            id -> id.contains("shard1") ? 0 : 1,
            shardIdx -> {
                Preconditions.checkArgument(shardIdx < 2);
                return shardIdx == 0 ? shard1 : shard2;
            },
            notificationSchedulerRef
    );

    @After
    public void tearDown() throws Exception {
        reconciler.close().block();
    }

    @Test(timeout = 30_000)
    public void testAdd() {
        Iterator<List<SimpleReconcilerEvent<String>>> eventsIt = reconciler.changes("junit").toIterable().iterator();
        assertThat(eventsIt.hasNext()).isTrue();
        assertThat(eventsIt.next()).isEmpty();

        // Shard1
        reconciler.add("abc_shard1", "value1").block();
        expectEvent(eventsIt, SimpleReconcilerEvent.Kind.Added, "abc_shard1", "value1");
        assertThat(shard1.findById("abc_shard1")).isNotNull();
        assertThat(shard2.getAll()).isEmpty();

        // Shard2
        reconciler.add("abc_shard2", "value2").block();
        expectEvent(eventsIt, SimpleReconcilerEvent.Kind.Added, "abc_shard2", "value2");
        assertThat(shard1.getAll()).hasSize(1);
        assertThat(shard2.findById("abc_shard1")).isNotNull();
    }

    @Test(timeout = 30_000)
    public void testApplyChange() {
        Iterator<List<SimpleReconcilerEvent<String>>> eventIt = addData("1@shard1", "2@shard1", "3@shard2", "4@shard2");

        reconciler.apply("1@shard1", data -> Mono.just("1")).block();
        expectEvent(eventIt, SimpleReconcilerEvent.Kind.Updated, "1@shard1", "1");
        assertThat(reconciler.getAll()).containsEntry("1@shard1", "1");

        reconciler.apply("4@shard2", data -> Mono.just("2")).block();
        expectEvent(eventIt, SimpleReconcilerEvent.Kind.Updated, "4@shard2", "2");
    }

    @Test(timeout = 30_000)
    public void testRemove() {
        Iterator<List<SimpleReconcilerEvent<String>>> eventId = addData("1@shard1", "2@shard1", "3@shard2", "4@shard2");
        reconciler.remove("1@shard1").block();
        expectEvent(eventId, SimpleReconcilerEvent.Kind.Removed, "1@shard1", "");
        reconciler.remove("3@shard2").block();
        expectEvent(eventId, SimpleReconcilerEvent.Kind.Removed, "3@shard2", "");
        assertThat(shard1.getAll()).hasSize(1).containsKey("2@shard1");
        assertThat(shard2.getAll()).hasSize(1).containsKey("4@shard2");
    }

    @Test(timeout = 30_000)
    public void testGetAllAndSize() {
        addData("1@shard1", "2@shard1", "3@shard2", "4@shard2");
        assertThat(reconciler.getAll()).containsKeys("1@shard1", "2@shard1", "3@shard2", "4@shard2");
        assertThat(reconciler.size()).isEqualTo(4);
    }

    @Test(timeout = 30_000)
    public void testFindById() {
        addData("1@shard1", "2@shard1", "3@shard2", "4@shard2");
        assertThat(reconciler.findById("1@shard1")).isNotEmpty();
        assertThat(reconciler.findById("4@shard2")).isNotEmpty();
        assertThat(reconciler.findById("wrongId")).isEmpty();
    }

    @Test
    public void testClose() {
        reconciler.close().block();
        try {
            reconciler.add("abc", "v").block();
            fail("Expected add failure");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IllegalStateException.class);
            assertThat(e.getMessage()).contains("Sharded reconciler closed");
        }
    }

    private Iterator<List<SimpleReconcilerEvent<String>>> addData(String... ids) {
        for (String id : ids) {
            reconciler.add(id, "").block();
        }
        Iterator<List<SimpleReconcilerEvent<String>>> eventsIt = reconciler.changes("junit").toIterable().iterator();
        assertThat(eventsIt.hasNext()).isTrue();
        List<SimpleReconcilerEvent<String>> next = eventsIt.next();
        assertThat(next).hasSize(ids.length);
        return eventsIt;
    }

    private void expectEvent(Iterator<List<SimpleReconcilerEvent<String>>> eventsIt,
                             SimpleReconcilerEvent.Kind kind, String id, String value) {
        assertThat(eventsIt.hasNext()).isTrue();
        List<SimpleReconcilerEvent<String>> next = eventsIt.next();
        assertThat(next).hasSize(1);
        assertThat(next.get(0).getKind()).isEqualTo(kind);
        assertThat(next.get(0).getId()).isEqualTo(id);
        assertThat(next.get(0).getData()).isEqualTo(value);
    }
}