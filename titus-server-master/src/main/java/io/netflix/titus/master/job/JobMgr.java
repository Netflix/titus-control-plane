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

package io.netflix.titus.master.job;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.master.Status;
import io.netflix.titus.master.model.job.TitusQueuableTask;
import io.netflix.titus.master.store.InvalidJobStateChangeException;
import io.netflix.titus.master.store.V2JobStore;
import org.apache.mesos.Protos;
import rx.subjects.ReplaySubject;

public interface JobMgr {

    String getJobId();

    ReplaySubject<Status> getStatusSubject();

    void updateInstances(int stageNum, int min, int desired, int max, String user) throws InvalidJobException;

    void setProcessStatus_TO_BE_RENAMED(int stage, boolean inService, String user) throws InvalidJobException;

    boolean isActive();

    void killJob(String user, String reason);

    boolean killTask(String taskId, String user, String reason);

    boolean killTaskAndShrink(String taskId, String user);

    void handleStatus(final Status status);

    Protos.TaskInfo setLaunchedAndCreateTaskInfo(TitusQueuableTask task, String hostname,
                                                 Map<String, String> attributeMap, Protos.SlaveID slaveID,
                                                 PreferentialNamedConsumableResourceSet.ConsumeResult consumedResourceSet,
                                                 List<Integer> portsAssigned)
            throws InvalidJobStateChangeException, InvalidJobException;

    long getTaskCreateTime(String taskId);

    void handleTaskStuckInState(String taskId, V2JobState state);

    void initialize(V2JobStore store);

    void initializeNewJob(final V2JobStore store) throws InvalidJobException;

    /**
     * To avoid race-condition between job construction process and Fenzo scheduling, we have to postpone adding
     * task to the Fenzo queue, until job initialization is complete and external reference map is updated. Only
     * when that happens this method should be called to submit init-time created tasks into Fenzo queue.
     */
    void postInitializeNewJob();

    void enforceSla();

    void setTaskKillAction(Consumer<String> killAction);

    void resubmitWorker(String taskId, String reason) throws InvalidJobException, InvalidJobStateChangeException;

    /**
     * Returns list of agents on which tasks of this job where run, and failed. There may be many additional criteria
     * to put an agent on this list, like task execution time (for example add to the list only if execution time < 60sec).
     * The agents on the list are not there permanently, just disabled for some time.
     */
    Set<String> getExcludedAgents();

    boolean isTaskValid(String taskId);

    void setMigrationDeadline(String taskId, long migrationDeadline);
}
