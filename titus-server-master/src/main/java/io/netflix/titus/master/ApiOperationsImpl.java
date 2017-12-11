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

package io.netflix.titus.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.job.V2JobMgrIntf;
import io.netflix.titus.master.job.service.ServiceJobMgr;
import io.netflix.titus.master.store.MetadataUtils;
import io.netflix.titus.master.store.NamedJob;
import io.netflix.titus.master.store.V2JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ApiOperationsImpl implements ApiOperations {

    private static final Logger logger = LoggerFactory.getLogger(ApiOperationsImpl.class);
    private ConcurrentHashMap<String, V2JobMgrIntf> jobStatusLookup;
    private final ConcurrentMap<String, NamedJob> namedJobsMap;
    private final VirtualMachineMasterService vmService;
    private final CountDownLatch isReadyLatch = new CountDownLatch(1);
    private volatile boolean isReadyState = false;
    private V2JobStore store = null;

    @Inject
    public ApiOperationsImpl(VirtualMachineMasterService vmService) {
        this.jobStatusLookup = new ConcurrentHashMap<>();
        this.namedJobsMap = new ConcurrentHashMap<>();
        this.vmService = vmService;
    }

    @Override
    public Set<String> getAllJobIds() {
        awaitReady();
        return new HashSet<>(jobStatusLookup.keySet());
    }

    @Override
    public List<V2JobMetadata> getAllJobsMetadata(final boolean activeOnly, int limit) {
        awaitReady();
        final Set<V2JobMetadata> filteredJobsMetadata = getFilteredJobsMetadata(activeOnly, jobMgr -> {
            if (jobMgr == null) {
                logger.warn("Job manager is null");
                return false;
            }
            if (jobMgr.getJobMetadata() == null) {
                logger.warn("JobMetadata is null for job manager {}", jobMgr.getJobId());
                return false;
            }
            return !(activeOnly && V2JobState.isTerminalState(jobMgr.getJobMetadata().getState()));
        });
        return getLimitedSet(filteredJobsMetadata, limit);
    }

    private List<V2JobMetadata> getLimitedSet(Set<V2JobMetadata> inputSet, int limit) {
        // We need to cache timestamp for each job, as it changes all the time, and breaks sorting order invariant.
        List<Pair<V2JobMetadata, Long>> sortedList = inputSet.stream()
                .map(j -> Pair.of(j, MetadataUtils.getLastActivityTime(j)))
                .collect(Collectors.toList());
        sortedList.sort(Comparator.comparingLong(Pair::getRight));

        int slice = limit <= 0 ? sortedList.size() : Math.min(sortedList.size(), limit);
        return sortedList.stream().limit(slice).map(Pair::getLeft).collect(Collectors.toList());
    }

    private Set<V2JobMetadata> getFilteredJobsMetadata(boolean activeOnly, final Predicate<V2JobMgrIntf> filter) {
        final Set<V2JobMetadata> jobMetadatas = jobStatusLookup.values().stream()
                .filter(filter)
                .map(V2JobMgrIntf::getJobMetadata)
                .collect(Collectors.toSet());
        if (!activeOnly) {
            jobMetadatas.addAll(
                    store.getTerminatedJobIds()
                            .stream()
                            .map(jobId -> {
                                try {
                                    return store.getCompletedJob(jobId);
                                } catch (IOException e) {
                                    return null;
                                }
                            })
                            .filter(m -> m != null)
                            .collect(Collectors.toSet())
            );
        }
        return jobMetadatas;
    }

    @Override
    public V2JobMetadata getJobMetadata(String jobId) {
        awaitReady();
        V2JobMgrIntf jobMgr = jobStatusLookup.get(jobId);
        if (jobMgr != null) {
            return jobMgr.getJobMetadata();
        }
        try {
            return store.getCompletedJob(jobId);
        } catch (IOException e) {
            logger.warn("Unexpected error getting completed job from store: " + e.getMessage(), e);
        }
        return null;
    }

    @Override
    public V2WorkerMetadata getWorker(String taskInstanceId) {
        for (V2JobMgrIntf jobManager : jobStatusLookup.values()) {
            for (V2WorkerMetadata worker : jobManager.getWorkers()) {
                if (taskInstanceId.equals(worker.getWorkerInstanceId())) {
                    return worker;
                }
            }
        }
        return null;
    }

    @Override
    public V2WorkerMetadata getWorker(String jobId, int number, boolean anyState) {
        V2JobMgrIntf jobMgr = jobStatusLookup.get(jobId);
        if (jobMgr != null) {
            final List<? extends V2WorkerMetadata> allRunningWorkers = anyState ? jobMgr.getWorkers() :
                    getRunningWorkers(jobMgr);
            for (V2WorkerMetadata w : allRunningWorkers) {
                if (w.getWorkerNumber() == number) {
                    return w;
                }
            }
        }
        final List<? extends V2WorkerMetadata> archivedWorkers = getArchivedWorkers(jobId);
        if (archivedWorkers == null || archivedWorkers.isEmpty()) {
            return null;
        }
        for (V2WorkerMetadata w : archivedWorkers) {
            if (w.getWorkerNumber() == number) {
                return w;
            }
        }
        return null;
    }

    private List<? extends V2WorkerMetadata> getRunningWorkers(V2JobMgrIntf jobMgr) {
        return jobMgr.getWorkers().stream().filter(t -> V2JobState.isRunningState(t.getState())).collect(Collectors.toList());
    }

    @Override
    public ConcurrentMap<String, NamedJob> getNamedJobs() {
        return namedJobsMap;
    }

    @Override
    public List<? extends V2WorkerMetadata> getArchivedWorkers(String jobId) {
        awaitReady();
        V2JobMgrIntf jobMgr = jobStatusLookup.get(jobId);
        if (jobMgr != null) {
            return jobMgr.getArchivedWorkers();
        }
        try {
            return store.getArchivedWorkers(jobId);
        } catch (IOException e) {
            logger.warn("Can't get archived workers for job " + jobId);
        }
        return null;
    }

    @Override
    public Set<String> getAllActiveJobs() {
        awaitReady();
        return new HashSet<>(jobStatusLookup.keySet());
    }

    @Override
    public List<V2WorkerMetadata> getAllWorkers(String jobId) {
        awaitReady();
        V2JobMgrIntf jobMgr = jobStatusLookup.get(jobId);
        if (jobMgr == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(jobMgr.getWorkers());
    }

    @Override
    public List<V2WorkerMetadata> getRunningWorkers(String jobId) {
        awaitReady();
        V2JobMgrIntf jobMgr = jobStatusLookup.get(jobId);
        if (jobMgr == null) {
            return new ArrayList<>();
        }
        return new LinkedList<>(getRunningWorkers(jobMgr));
    }

    @Override
    public boolean killJob(String jobId, String user) {
        awaitReady();
        V2JobMgrIntf jobMgr = jobStatusLookup.get(jobId);
        if (jobMgr == null) {
            return false;
        }
        jobMgr.killJob(user, "user requested");
        return true;
    }

    @Override
    public boolean killWorker(String taskId, String user) {
        awaitReady();
        final V2JobMgrIntf jobMgr = jobStatusLookup.get(WorkerNaming.getJobAndWorkerId(taskId).jobId);
        return jobMgr != null && jobMgr.killTask(taskId, user, "");
    }

    @Override
    public boolean killWorkerAndShrink(String taskId, String user) {
        awaitReady();
        final V2JobMgrIntf jobMgr = jobStatusLookup.get(WorkerNaming.getJobAndWorkerId(taskId).jobId);
        if (jobMgr == null) {
            return false;
        }
        return jobMgr.killTaskAndShrink(taskId, user);
    }

    @Override
    public void updateInstanceCounts(String jobId, int stageNum, int min, int desired, int max, String user) throws InvalidJobException {
        awaitReady();
        V2JobMgrIntf jobMgr = jobStatusLookup.get(jobId);
        if (jobMgr == null) {
            throw new InvalidJobException(jobId);
        }
        jobMgr.updateInstances(stageNum, min, desired, max, user);
    }

    @Override
    public void updateJobProcesses(String jobId, int stageNum, boolean disableIncreaseDesired, boolean disableDecreaseDesired, String user) throws InvalidJobException {
        awaitReady();
        V2JobMgrIntf jobMgr = jobStatusLookup.get(jobId);
        if (jobMgr == null) {
            throw new InvalidJobException(jobId);
        }

        if (!(jobMgr instanceof ServiceJobMgr)) {
            throw JobManagerException.notServiceJob(jobId);
        }

        ServiceJobMgr serviceJobMgr = (ServiceJobMgr) jobMgr;
        serviceJobMgr.updateJobProcesses(stageNum, disableIncreaseDesired, disableDecreaseDesired, user);
    }

    @Override
    public void updateInServiceStatus(String jobId, int stage, boolean inService, String user) throws InvalidJobException {
        awaitReady();
        V2JobMgrIntf jobMgr = jobStatusLookup.get(jobId);
        if (jobMgr == null) {
            throw new InvalidJobException(jobId);
        }
        jobMgr.setProcessStatus_TO_BE_RENAMED(stage, inService, user);
    }

    private void awaitReady() {
        try {
            while (!isReadyLatch.await(10, TimeUnit.SECONDS)) {
                logger.info("Waiting for getting to ready state");
            }
        } catch (InterruptedException e) {
            logger.warn("Ignoring interruption waiting for isReady: " + e.getMessage());
        }
    }

    public boolean isReady() {
        return isReadyState;
    }

    @Override
    public void setReady(V2JobStore store) {
        logger.info("setting isReady");
        this.store = store;
        isReadyLatch.countDown();
        isReadyState = true;
    }

    @Override
    public void removeJobIdRef(String jobId) {
        jobStatusLookup.remove(jobId);
    }

    @Override
    public void addJobIdRef(String jobId, V2JobMgrIntf jobMgr) {
        jobStatusLookup.put(jobId, jobMgr);
    }
}
