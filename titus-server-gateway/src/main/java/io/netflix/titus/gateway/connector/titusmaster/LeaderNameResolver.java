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

package io.netflix.titus.gateway.connector.titusmaster;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Preconditions;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import io.grpc.internal.LogExceptionRunnable;
import io.grpc.internal.SharedResourceHolder;

public class LeaderNameResolver extends NameResolver {
    private static final int DELAY_MS = 5_000;

    private final String authority;
    private final LeaderResolver leaderResolver;
    private final int port;
    private final SharedResourceHolder.Resource<ScheduledExecutorService> timerServiceResource;
    private final SharedResourceHolder.Resource<ExecutorService> executorResource;
    @GuardedBy("this")
    private boolean shutdown;
    @GuardedBy("this")
    private ScheduledExecutorService timerService;
    @GuardedBy("this")
    private ExecutorService executor;
    @GuardedBy("this")
    private ScheduledFuture<?> resolutionTask;
    @GuardedBy("this")
    private boolean resolving;
    @GuardedBy("this")
    private Listener listener;

    public LeaderNameResolver(URI targetUri,
                              LeaderResolver leaderResolver,
                              int port,
                              SharedResourceHolder.Resource<ScheduledExecutorService> timerServiceResource,
                              SharedResourceHolder.Resource<ExecutorService> executorResource) {

        this.leaderResolver = leaderResolver;
        this.port = port;
        this.timerServiceResource = timerServiceResource;
        this.executorResource = executorResource;

        if (targetUri.getAuthority() != null) {
            this.authority = targetUri.getAuthority();
        } else {
            this.authority = targetUri.getPath().substring(1);
        }
    }

    @Override
    public String getServiceAuthority() {
        return this.authority;
    }

    @Override
    public final synchronized void start(Listener listener) {
        Preconditions.checkState(this.listener == null, "already started");
        timerService = SharedResourceHolder.get(timerServiceResource);
        executor = SharedResourceHolder.get(executorResource);
        this.listener = Preconditions.checkNotNull(listener, "listener");
        resolve();
    }

    @Override
    public final synchronized void refresh() {
        Preconditions.checkState(listener != null, "not started");
        resolve();
    }

    private final Runnable resolutionRunnable = new Runnable() {
        @Override
        public void run() {
            Listener savedListener;
            synchronized (LeaderNameResolver.this) {
                // If this task is started by refresh(), there might already be a scheduled task.
                if (resolutionTask != null) {
                    resolutionTask.cancel(false);
                    resolutionTask = null;
                }
                if (shutdown) {
                    return;
                }
                savedListener = listener;
                resolving = true;
            }
            try {
                Optional<Address> leaderOptional = leaderResolver.resolve();
                if (!leaderOptional.isPresent()) {
                    synchronized (LeaderNameResolver.this) {
                        if (shutdown) {
                            return;
                        }
                        // Because timerService is the single-threaded GrpcUtil.TIMER_SERVICE in production,
                        // we need to delegate the blocking work to the executor
                        resolutionTask = timerService.schedule(new LogExceptionRunnable(resolutionRunnableOnExecutor),
                                DELAY_MS, TimeUnit.MILLISECONDS);
                    }
                    savedListener.onError(Status.UNAVAILABLE.withDescription("No server returned from leader resolver"));
                    return;
                }

                Address address = leaderOptional.get();
                EquivalentAddressGroup servers = new EquivalentAddressGroup(new InetSocketAddress(address.getHost(), port));
                savedListener.onAddresses(Collections.singletonList(servers), Attributes.EMPTY);
            } finally {
                synchronized (LeaderNameResolver.this) {
                    resolving = false;
                }
            }
        }
    };

    private final Runnable resolutionRunnableOnExecutor = new Runnable() {
        @Override
        public void run() {
            synchronized (LeaderNameResolver.this) {
                if (!shutdown) {
                    executor.execute(resolutionRunnable);
                }
            }
        }
    };

    @GuardedBy("this")
    private void resolve() {
        if (resolving || shutdown) {
            return;
        }
        executor.execute(resolutionRunnable);
    }

    @Override
    public final synchronized void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;
        if (resolutionTask != null) {
            resolutionTask.cancel(false);
        }
        if (timerService != null) {
            timerService = SharedResourceHolder.release(timerServiceResource, timerService);
        }
        if (executor != null) {
            executor = SharedResourceHolder.release(executorResource, executor);
        }
    }
}