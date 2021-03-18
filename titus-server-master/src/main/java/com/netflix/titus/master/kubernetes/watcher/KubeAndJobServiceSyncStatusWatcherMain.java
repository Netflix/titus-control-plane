/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.watcher;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.mesos.kubeapiserver.DefaultContainerResultCodeResolver;
import com.netflix.titus.runtime.connector.kubernetes.DefaultKubeApiFacade;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiClients;
import com.netflix.titus.runtime.connector.kubernetes.KubeConnectorConfiguration;
import io.kubernetes.client.openapi.ApiClient;
import rx.Observable;

/**
 * Help CLI utility to watch informer event streams.
 */
public class KubeAndJobServiceSyncStatusWatcherMain {

    private static final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private static final KubeConnectorConfiguration kubeConnectorConfiguration = Archaius2Ext.newConfiguration(KubeConnectorConfiguration.class);

    private static final MesosConfiguration mesosConfig = Archaius2Ext.newConfiguration(MesosConfiguration.class);

    private static final DefaultContainerResultCodeResolver containerResultCodeResolver = new DefaultContainerResultCodeResolver(mesosConfig);

    private static final ReadOnlyJobOperations jobService = new ReadOnlyJobOperations() {
        @Override
        public List<Job> getJobs() {
            return null;
        }

        @Override
        public Optional<Job<?>> getJob(String jobId) {
            return Optional.empty();
        }

        @Override
        public List<Task> getTasks() {
            return null;
        }

        @Override
        public List<Task> getTasks(String jobId) {
            return null;
        }

        @Override
        public List<Pair<Job, List<Task>>> getJobsAndTasks() {
            return Collections.emptyList();
        }

        @Override
        public List<Job<?>> findJobs(Predicate<Pair<Job<?>, List<Task>>> queryPredicate, int offset, int limit) {
            return null;
        }

        @Override
        public List<Pair<Job<?>, Task>> findTasks(Predicate<Pair<Job<?>, Task>> queryPredicate, int offset, int limit) {
            return null;
        }

        @Override
        public Optional<Pair<Job<?>, Task>> findTaskById(String taskId) {
            return Optional.empty();
        }

        @Override
        public Observable<JobManagerEvent<?>> observeJobs(Predicate<Pair<Job<?>, List<Task>>> jobsPredicate, Predicate<Pair<Job<?>, Task>> tasksPredicate) {
            return null;
        }

        @Override
        public Observable<JobManagerEvent<?>> observeJob(String jobId) {
            return null;
        }
    };

    public static void main(String[] args) {
        ApiClient kubeClient = KubeApiClients.createApiClient("cli", TitusRuntimes.internal(), 0L);
        DefaultKubeApiFacade facade = new DefaultKubeApiFacade(kubeConnectorConfiguration, kubeClient, titusRuntime);

        KubeAndJobServiceSyncStatusWatcher watcher = new KubeAndJobServiceSyncStatusWatcher(facade, jobService, containerResultCodeResolver, titusRuntime);
        watcher.enterActiveMode();
        try {
            Thread.sleep(3600_1000);
        } catch (InterruptedException ignore) {
        }
    }
}
