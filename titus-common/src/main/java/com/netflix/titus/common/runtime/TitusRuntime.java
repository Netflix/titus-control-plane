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

package com.netflix.titus.common.runtime;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.code.CodePointTracker;
import com.netflix.titus.common.util.time.Clock;
import rx.Observable;

/**
 * Collection of core services used by all Titus components.
 */
public interface TitusRuntime {
    /**
     * Returns the configured {@link CodePointTracker} instance.
     */
    CodePointTracker getCodePointTracker();

    /**
     * Returns the configured {@link CodeInvariants} instance.
     */
    CodeInvariants getCodeInvariants();

    /**
     * Returns the configured {@link SystemLogService}.
     */
    SystemLogService getSystemLogService();

    /**
     * Returns the configured Spectator registry.
     */
    Registry getRegistry();

    /**
     * Instruments the given observable with metrics, and applies default retry strategy if the observable completes
     * with an error. Its primary purpose is to use for tracking inter-component subscriptions, which should exist
     * for the full lifetime of the process.
     */
    <T> Observable<T> persistentStream(Observable<T> source);

    /**
     * Returns the configured clock.
     */
    Clock getClock();

    /**
     * Returns FIT framework.
     */
    FitFramework getFitFramework();

    /**
     * Simple in JVM task scheduler.
     */
    LocalScheduler getLocalScheduler();

    /**
     * If true, fatal errors cause JVM termination via System.exit.
     */
    boolean isSystemExitOnFailure();

    /**
     * In a few places in TitusMaster a component may decide to break the bootstrap process or terminate the JVM process
     * abruptly. This method should be called before that happens, so the {@link SystemAbortListener} implementation can
     * send a signal/alert to an administrator about the incident.
     */
    void beforeAbort(SystemAbortEvent event);
}
