/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.testkit.embedded;

import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class EmbeddedTitusCellTest {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedTitusCellTest.class);

    private static final String[] OK_THREAD_GROUPS = {
            "system"
    };

    private static final String[] OK_THREADS = {
            // Java
            "Timer.*",
            // RxJava
            "RxComputationScheduler.*",
            "RxIoScheduler.*",
            "RxNewThreadScheduler.*",
            "RxScheduledExecutorPool.*",
            // Netty
            "globalEventExecutor.*",
            "threadDeathWatcher.*",
            // RxNetty
            "rxnetty-nio-eventloop.*",
            "global-client-idle-conn-cleanup-scheduler.*",
            // Grpc
            "grpc-shared-destroyer.*",
            "grpc-default-boss.*",
            // Governator
            // It is shutting down shortly after injector is stopped, which causes test flakiness
            "lifecycle-listener-monitor.*",
            //Spectator TODO should this be here?
            "spectator.*"
    };

    @Test
    @Ignore
    public void testShutdownCleanup() throws Throwable {
        Set<Thread> threadsBefore = Thread.getAllStackTraces().keySet();

        EmbeddedTitusCell titusStack = EmbeddedTitusCells.basicCell(1);
        titusStack.boot();
        titusStack.shutdown();

        Set<Thread> threadsAfter = Thread.getAllStackTraces().keySet();

        // Filter-out non Titus threads
        Set<Thread> titusRemainingThreads = threadsAfter.stream()
                .filter(t -> {
                    ThreadGroup threadGroup = t.getThreadGroup();
                    if (threadGroup == null) { // if thread died
                        return false;
                    }
                    return !matches(OK_THREAD_GROUPS, threadGroup.getName());
                })
                .filter(t -> !matches(OK_THREADS, t.getName()))
                .collect(Collectors.toSet());

        CollectionsExt.copyAndRemove(titusRemainingThreads, threadsBefore).forEach(t -> {
                    logger.info("Unexpected thread {}:\n{}",
                            t.getName(),
                            String.join("\n", Arrays.stream(t.getStackTrace()).map(e -> "  " + e).collect(Collectors.toList()))
                    );
                }
        );
        assertThat(titusRemainingThreads).isSubsetOf(threadsBefore);
    }

    private boolean matches(String[] patterns, String text) {
        for (String pattern : patterns) {
            if (Pattern.matches(pattern, text)) {
                return true;
            }
        }
        return false;
    }
}