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

package com.netflix.titus.common.network.socket;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.NetworkExt;
import com.netflix.titus.common.util.ReflectionExt;
import com.netflix.titus.common.util.StringExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnusedSocketPortAllocator implements SocketPortAllocator {

    private static final Logger logger = LoggerFactory.getLogger(UnusedSocketPortAllocator.class);

    private static final UnusedSocketPortAllocator INSTANCE = new UnusedSocketPortAllocator();

    /**
     * For JVM running concurrently (integration tests), we want to place their port allocation in different pools.
     */
    private static final int PARTITION = 100;
    private static final int HISTORY_SIZE = PARTITION / 2;

    private static final int PID = getPid();

    private final int offset;

    // We use list not set, as the order is important
    private final List<Integer> recentAllocations = new ArrayList<>();

    private int next;

    private UnusedSocketPortAllocator() {
        this.offset = NetworkExt.findUnusedPort() + (PID % 10) * PARTITION;
    }

    @Override
    public int allocate() {
        synchronized (recentAllocations) {
            for (int i = 0; i < PARTITION; i++) {
                int port = offset + next;
                next = (next + 1) % PARTITION;

                if (!isRecentlyUsed(port) && isAvailable(port)) {
                    recentAllocations.add(port);
                    while (recentAllocations.size() > HISTORY_SIZE) {
                        recentAllocations.remove(0);
                    }
                    logger.info("Allocated port: port={} caller={}", port,
                            ReflectionExt.findCallerStackTrace().map(StackTraceElement::toString).orElse("unknown")
                    );
                    return port;
                }
            }
        }
        throw new IllegalStateException("No free sockets available");
    }

    public static UnusedSocketPortAllocator global() {
        return INSTANCE;
    }

    private boolean isRecentlyUsed(int port) {
        for (int used : recentAllocations) {
            if (used == port) {
                return true;
            }
        }
        return false;
    }

    private boolean isAvailable(int port) {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static int getPid() {
        int pid;

        // Try sun system property first
        String property = System.getProperty("sun.java.launcher.pid");
        if (property != null) {
            pid = ExceptionExt.doTry(() -> Integer.parseInt(property)).orElse(-1);
            if (pid >= 0) {
                return pid;
            }
        }

        // Now JMX
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        if (StringExt.isNotEmpty(processName)) {
            StringBuilder digitsOnly = new StringBuilder();
            for (char c : processName.toCharArray()) {
                if (Character.isDigit(c)) {
                    digitsOnly.append(c);
                }
            }
            if (digitsOnly.length() > 0) {
                String digits = digitsOnly.substring(0, Math.min(8, digitsOnly.length()));
                return Integer.parseInt(digits);
            }
        }

        // Final fallback - a random number.
        return new Random().nextInt(10);
    }
}
