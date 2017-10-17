/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.util;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * A collection of network-level helper functions.
 */
public final class NetworkExt {

    private NetworkExt() {
    }

    public static int findUnusedPort() {
        int port;
        try {
            ServerSocket serverSocket = new ServerSocket(0);
            port = serverSocket.getLocalPort();
            serverSocket.close();
        } catch (IOException e) {
            throw new IllegalStateException("Unused port allocation failure", e);
        }
        return port;
    }
}
