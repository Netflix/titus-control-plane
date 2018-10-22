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

import java.util.function.Supplier;

import com.google.common.base.Preconditions;

public class DirectSocketPortAllocator implements SocketPortAllocator {

    private final Supplier<Integer> portSupplier;

    public DirectSocketPortAllocator(Supplier<Integer> portSupplier) {
        this.portSupplier = portSupplier;
    }

    @Override
    public int allocate() {
        int port = portSupplier.get();
        Preconditions.checkState(port > 0, "Allocated server socket port is <= 0: %s", port);
        return port;
    }
}
