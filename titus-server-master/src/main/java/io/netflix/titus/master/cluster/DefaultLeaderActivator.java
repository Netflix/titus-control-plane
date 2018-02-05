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

package io.netflix.titus.master.cluster;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.Injector;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.util.guice.ActivationLifecycle;
import io.netflix.titus.common.util.guice.ContainerEventBus;
import io.netflix.titus.common.util.guice.ContainerEventBus.ContainerEventListener;
import io.netflix.titus.common.util.guice.ContainerEventBus.ContainerStartedEvent;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.scheduler.SchedulingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Singleton
public class DefaultLeaderActivator implements LeaderActivator, ContainerEventListener<ContainerStartedEvent> {

    /**
     * We need to account for situation where {@link #becomeLeader()} is called before container setup is finished.
     */
    private enum State {
        Starting, Started, Leader, StartedLeader
    }

    private static final Logger logger = LoggerFactory.getLogger(DefaultLeaderActivator.class);

    private final AtomicReference<State> stateRef = new AtomicReference<>(State.Starting);

    private final Injector injector;
    private final ActivationLifecycle activationLifecycle;

    private final AtomicInteger isLeaderGauge;
    private final AtomicInteger isActivatedGauge;
    private final AtomicLong activationTimeGauge;

    private volatile boolean activated;
    private volatile long electionTime = -1;
    private volatile long activationTime = -1;

    @Inject
    public DefaultLeaderActivator(Injector injector,
                                  ContainerEventBus eventBus,
                                  ActivationLifecycle activationLifecycle,
                                  Registry registry) {
        this.injector = injector;
        this.activationLifecycle = activationLifecycle;
        this.isLeaderGauge = registry.gauge(MetricConstants.METRIC_LEADER + "isLeaderGauge", new AtomicInteger());
        this.isActivatedGauge = registry.gauge(MetricConstants.METRIC_LEADER + "isActivatedGauge", new AtomicInteger());
        this.activationTimeGauge = registry.gauge(MetricConstants.METRIC_LEADER + "activationTime", new AtomicLong());
        eventBus.registerListener(this);
    }

    @Override
    public long getElectionTime() {
        return electionTime;
    }

    @Override
    public long getActivationTime() {
        return activationTime;
    }

    @Override
    public boolean isLeader() {
        State state = stateRef.get();
        return state == State.Leader || state == State.StartedLeader;
    }

    @Override
    public boolean isActivated() {
        return activated;
    }

    @Override
    public void becomeLeader() {
        logger.info("Becoming leader now");
        if (stateRef.compareAndSet(State.Starting, State.Leader)) {
            isLeaderGauge.set(1);
            electionTime = System.currentTimeMillis();
            return;
        }
        if (stateRef.compareAndSet(State.Started, State.StartedLeader)) {
            isLeaderGauge.set(1);
            electionTime = System.currentTimeMillis();
            activate();
        }
        logger.warn("Unexpected to be told to enter leader mode more than once, ignoring.");
    }

    @Override
    public void stopBeingLeader() {
        logger.info("Asked to stop being leader now");
        isLeaderGauge.set(0);
        isActivatedGauge.set(0);

        if (!isLeader()) {
            logger.warn("Unexpected to be told to stop being leader when we haven't entered leader mode before, ignoring.");
            return;
        }

        // Various services may have built in-memory state that is currently not easy to revert to initialization state.
        // Until we create such a lifecycle feature for each service and all of their references, best thing to do is to
        //  exit the process and depend on a watcher process to restart us right away. Especially since restart isn't
        // very expensive.
        logger.error("Exiting due to losing leadership after running as leader");
        System.exit(1);
    }

    @Override
    public void onEvent(ContainerStartedEvent event) {
        if (stateRef.compareAndSet(State.Starting, State.Started)) {
            return;
        }
        if (stateRef.compareAndSet(State.Leader, State.StartedLeader)) {
            activate();
        }
        logger.warn("ContainerStartedEvent received while in state {}; ignoring", stateRef.get());
    }

    private void activate() {
        activationLifecycle.activate();
        injector.getInstance(SchedulingService.class).startScheduling();
        isActivatedGauge.set(1);
        activated = true;
        activationTime = System.currentTimeMillis();
        activationTimeGauge.set(activationLifecycle.getActivationTimeMs());
    }
}
