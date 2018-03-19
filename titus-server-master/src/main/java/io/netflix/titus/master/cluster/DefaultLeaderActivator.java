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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.Injector;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import io.netflix.titus.common.framework.fit.FitFramework;
import io.netflix.titus.common.framework.fit.FitInjection;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.guice.ActivationLifecycle;
import io.netflix.titus.common.util.guice.ContainerEventBus;
import io.netflix.titus.common.util.guice.ContainerEventBus.ContainerEventListener;
import io.netflix.titus.common.util.guice.ContainerEventBus.ContainerStartedEvent;
import io.netflix.titus.common.util.time.Clock;
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
    private final Clock clock;
    private final ActivationLifecycle activationLifecycle;

    private volatile boolean leader;
    private volatile boolean activated;

    private volatile long electionTimestamp = -1;
    private volatile long activationStartTimestamp = -1;
    private volatile long activationEndTimestamp = -1;
    private volatile long activationTime = -1;

    private final Optional<FitInjection> beforeActivationFitInjection;

    @Inject
    public DefaultLeaderActivator(Injector injector,
                                  ContainerEventBus eventBus,
                                  ActivationLifecycle activationLifecycle,
                                  TitusRuntime titusRuntime) {
        this.injector = injector;
        this.activationLifecycle = activationLifecycle;
        this.clock = titusRuntime.getClock();

        Registry registry = titusRuntime.getRegistry();

        PolledMeter.using(registry)
                .withName(MetricConstants.METRIC_LEADER + "isLeaderGauge")
                .monitorValue(this, self -> leader ? 1 : 0);

        PolledMeter.using(registry)
                .withName(MetricConstants.METRIC_LEADER + "isActivatedGauge")
                .monitorValue(this, self -> activated ? 1 : 0);

        PolledMeter.using(registry)
                .withName(MetricConstants.METRIC_LEADER + "activationTime")
                .monitorValue(this, self -> getActivationTime());

        PolledMeter.using(registry)
                .withName(MetricConstants.METRIC_LEADER + "inActiveStateTime")
                .monitorValue(this, self -> isActivated() ? clock.wallTime() - activationEndTimestamp : 0L);

        FitFramework fit = titusRuntime.getFitFramework();
        if (fit.isActive()) {
            FitInjection beforeActivationFitInjection = fit.newFitInjectionBuilder("beforeActivation")
                    .withDescription("Inject failures after the node becomes the leader, but before the activation process is started")
                    .build();
            fit.getRootComponent().getChild(COMPONENT).addInjection(beforeActivationFitInjection);

            this.beforeActivationFitInjection = Optional.of(beforeActivationFitInjection);
        } else {
            this.beforeActivationFitInjection = Optional.empty();
        }

        eventBus.registerListener(this);
    }

    @Override
    public long getElectionTimestamp() {
        return electionTimestamp;
    }

    @Override
    public long getActivationEndTimestamp() {
        return activationEndTimestamp;
    }

    @Override
    public long getActivationTime() {
        if (isActivated()) {
            return activationTime;
        }
        if (!isLeader()) {
            return -1;
        }
        return clock.wallTime() - activationStartTimestamp;
    }

    @Override
    public boolean isLeader() {
        return leader;
    }

    @Override
    public boolean isActivated() {
        return activated;
    }

    @Override
    public void becomeLeader() {
        logger.info("Becoming leader now");
        if (stateRef.compareAndSet(State.Starting, State.Leader)) {
            leader = true;
            electionTimestamp = clock.wallTime();
            return;
        }
        if (stateRef.compareAndSet(State.Started, State.StartedLeader)) {
            leader = true;
            electionTimestamp = clock.wallTime();
            activate();
            return;
        }
        logger.warn("Unexpected to be told to enter leader mode more than once, ignoring.");
    }

    @Override
    public void stopBeingLeader() {
        logger.info("Asked to stop being leader now");

        if (!leader) {
            logger.warn("Unexpected to be told to stop being leader when we haven't entered leader mode before, ignoring.");
            return;
        }

        leader = false;
        activated = false;

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
            return;
        }
        logger.warn("ContainerStartedEvent received while in state {}; ignoring", stateRef.get());
    }

    private void activate() {
        this.activationStartTimestamp = clock.wallTime();

        beforeActivationFitInjection.ifPresent(i -> i.beforeImmediate("beforeActivation"));

        try {
            try {
                activationLifecycle.activate();
                injector.getInstance(SchedulingService.class).startScheduling();
            } catch (Exception e) {
                stopBeingLeader();

                // As stopBeingLeader method not always terminates the process, lets make sure it does.
                System.exit(-1);
            }
        } catch (Throwable e) {
            System.exit(-1);
        }

        this.activated = true;
        this.activationEndTimestamp = clock.wallTime();
        this.activationTime = activationEndTimestamp - activationStartTimestamp;
    }
}
