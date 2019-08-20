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

package com.netflix.titus.master.model.job;

import com.netflix.fenzo.queues.QueuableTask;

/**
 * Titus extension of Fenzo's {@link QueuableTask} API.
 */
public interface TitusQueuableTask<JOB, TASK> extends QueuableTask {

    /**
     * Titus job instance owning this task.
     */
    JOB getJob();

    /**
     * Titus task instance associated with the given Fenzo task.
     */
    TASK getTask();

    /**
     * Is opportunistic usage of CPUs enabled for this task?
     *
     * @return true when opportunistic scheduling of CPUs is enabled for a particular task
     */
    boolean isCpuOpportunistic();

    /**
     * The current amount of opportunistic CPUs requested by a task. Note that this may change after a task is notified
     * that a scheduling iteration was not able to allocate the requested amount of opportunistic CPUs.
     *
     * @see TitusQueuableTask#opportunisticSchedulingFailed()
     */
    int getOpportunisticCpus();

    /**
     * Notify that an iteration loop using opportunistic resources failed. Implementations can use the notification to
     * adjust opportunistic resource asks for the next scheduling iteration.
     */
    void opportunisticSchedulingFailed();
}
