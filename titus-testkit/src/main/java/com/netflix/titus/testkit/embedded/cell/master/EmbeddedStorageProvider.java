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

package com.netflix.titus.testkit.embedded.cell.master;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.api.store.v2.InvalidJobException;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2StageMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.master.store.InvalidNamedJobException;
import com.netflix.titus.master.store.JobAlreadyExistsException;
import com.netflix.titus.master.store.JobNameAlreadyExistsException;
import com.netflix.titus.master.store.NamedJob;
import com.netflix.titus.master.store.V2JobMetadataWritable;
import com.netflix.titus.master.store.V2StageMetadataWritable;
import com.netflix.titus.master.store.V2StorageProvider;
import com.netflix.titus.master.store.V2WorkerMetadataWritable;
import rx.Observable;

/**
 */
public class EmbeddedStorageProvider implements V2StorageProvider {

    private static final TypeReference<List<V2JobMetadataWritable>> JOB_METADATA_LIST = new TypeReference<List<V2JobMetadataWritable>>() {
    };

    private static final TypeReference<List<V2StageMetadataWritable>> JOB_STAGE_METADATA_LIST = new TypeReference<List<V2StageMetadataWritable>>() {
    };

    private static final TypeReference<List<V2WorkerMetadataWritable>> WORKER_METADATA_LIST = new TypeReference<List<V2WorkerMetadataWritable>>() {
    };

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Map<String, NamedJob> namedJobsById = new ConcurrentHashMap<>();

    private ConcurrentMap<String, V2JobMetadataWritable> jobMetadataStore = new ConcurrentHashMap<>();
    private ConcurrentMap<String, V2JobMetadataWritable> archivedJobMetadataStore = new ConcurrentHashMap<>();
    private ConcurrentMap<String, V2WorkerMetadataWritable> workerMetadataStore = new ConcurrentHashMap<>();
    private ConcurrentMap<String, V2WorkerMetadataWritable> archiveWorkerStore = new ConcurrentHashMap<>();
    private ConcurrentMap<String, V2StageMetadataWritable> stageStore = new ConcurrentHashMap<>();
    private ConcurrentMap<String, V2StageMetadataWritable> archivedStageStore = new ConcurrentHashMap<>();
    private List<String> vmAttributesList = Collections.emptyList();

    public EmbeddedStorageProvider() {
    }

    public EmbeddedStorageProvider(Map<String, V2JobMetadataWritable> jobMetadataStore,
                                   Map<String, V2StageMetadataWritable> stageStore,
                                   Map<String, V2WorkerMetadataWritable> workerMetadataStore) {
        this.jobMetadataStore = new ConcurrentHashMap<>(jobMetadataStore);
        this.workerMetadataStore = new ConcurrentHashMap<>(workerMetadataStore);
        this.stageStore = new ConcurrentHashMap<>(stageStore);
    }

    @Override
    public List<V2JobMetadataWritable> initJobs() throws IOException {
        for (String stageId : stageStore.keySet()) {
            V2StageMetadataWritable reloadedStage = reloadStage(stageStore.get(stageId));
            V2JobMetadataWritable reloadedJob = reloadWithStage(jobMetadataStore.get(reloadedStage.getJobId()), reloadedStage);
            jobMetadataStore.put(reloadedJob.getJobId(), reloadedJob);
            stageStore.put(stageId, reloadedStage);
        }

        for (String workerId : workerMetadataStore.keySet()) {
            V2WorkerMetadataWritable reloadedWorker = reloadWorker(workerMetadataStore.get(workerId));
            workerMetadataStore.put(workerId, reloadedWorker);
            try {
                jobMetadataStore.get(reloadedWorker.getJobId()).addWorkerMedata(1, reloadedWorker, null);
            } catch (InvalidJobException e) {
                throw new IOException("Cannot restore job " + reloadedWorker.getJobId());
            }
        }
        for (String workerId : archiveWorkerStore.keySet()) {
            archiveWorkerStore.put(workerId, reloadWorker(archiveWorkerStore.get(workerId)));
        }

        List<V2JobMetadataWritable> result = new ArrayList<>();
        jobMetadataStore.values().forEach(result::add);
        return result;
    }

    @Override
    public Observable<V2JobMetadata> initArchivedJobs() {
        return Observable.create(subscriber -> {
            try {
                for (String stageId : archivedStageStore.keySet()) {
                    V2StageMetadataWritable reloadedStage = reloadStage(archivedStageStore.get(stageId));
                    V2JobMetadataWritable reloadedJob = reloadWithStage(archivedJobMetadataStore.get(reloadedStage.getJobId()), reloadedStage);
                    archivedJobMetadataStore.put(reloadedJob.getJobId(), reloadedJob);
                    archivedStageStore.put(stageId, reloadedStage);
                }
                for (String workerId : archiveWorkerStore.keySet()) {
                    V2WorkerMetadataWritable reloadedWorker = reloadWorker(archiveWorkerStore.get(workerId));
                    archiveWorkerStore.put(workerId, reloadedWorker);
                    try {
                        archivedJobMetadataStore.get(reloadedWorker.getJobId()).addWorkerMedata(1, reloadedWorker, null);
                    } catch (InvalidJobException e) {
                        subscriber.onError(new IOException("Cannot restore job " + reloadedWorker.getJobId()));
                    }
                }
                for (String workerId : archiveWorkerStore.keySet()) {
                    archiveWorkerStore.put(workerId, reloadWorker(archiveWorkerStore.get(workerId)));
                }
                archivedJobMetadataStore.values().forEach(subscriber::onNext);
            } finally {
                subscriber.onCompleted();
            }
        });
    }

    @Override
    public List<NamedJob> initNamedJobs() throws IOException {
        return new ArrayList<>(namedJobsById.values());
    }

    public V2JobMetadataWritable getJob(String jobId) {
        return jobMetadataStore.get(jobId);
    }

    @Override
    public Observable<NamedJob.CompletedJob> initNamedJobCompletedJobs() throws IOException {
        return Observable.empty(); // not needed at this time
    }

    @Override
    public void storeNewJob(V2JobMetadataWritable jobMetadata) throws JobAlreadyExistsException, IOException {
        jobMetadataStore.put(jobMetadata.getJobId(), jobMetadata);
    }

    @Override
    public void updateJob(V2JobMetadataWritable jobMetadata) throws InvalidJobException, IOException {
        jobMetadataStore.put(jobMetadata.getJobId(), jobMetadata);
    }

    @Override
    public void archiveJob(String jobId) throws IOException {
        final V2JobMetadataWritable removed = jobMetadataStore.remove(jobId);
        if (removed != null) {
            archivedJobMetadataStore.put(jobId, removed);
            for (V2StageMetadata s : removed.getStageMetadata()) {
                final V2StageMetadataWritable stage = stageStore.remove(getStageId((V2StageMetadataWritable) s));
                if (stage != null) {
                    archivedStageStore.put(getStageId((V2StageMetadataWritable) s), (V2StageMetadataWritable) s);
                }
                for (V2WorkerMetadata w : s.getAllWorkers()) {
                    final V2WorkerMetadataWritable rw = workerMetadataStore.remove(getWorkerId((V2WorkerMetadataWritable) w));
                    if (rw != null) {
                        archiveWorkerStore.put(getWorkerId((V2WorkerMetadataWritable) w), (V2WorkerMetadataWritable) w);
                    }
                }
            }
        }
    }

    @Override
    public void deleteJob(String jobId) throws InvalidJobException, IOException {
        jobMetadataStore.remove(jobId);
        archivedJobMetadataStore.remove(jobId);
    }

    @Override
    public void storeStage(V2StageMetadataWritable msmd) throws IOException {
        stageStore.put(getStageId(msmd), msmd);
    }

    @Override
    public void updateStage(V2StageMetadataWritable msmd) throws IOException {
        stageStore.put(getStageId(msmd), msmd);
    }

    @Override
    public void storeWorker(V2WorkerMetadataWritable workerMetadata) throws IOException {
        workerMetadataStore.put(getWorkerId(workerMetadata), workerMetadata);
    }

    @Override
    public void storeWorkers(String jobId, List<V2WorkerMetadataWritable> workers) throws IOException {
        for (V2WorkerMetadataWritable worker : workers) {
            storeWorker(worker);
        }
    }

    @Override
    public void storeAndUpdateWorkers(V2WorkerMetadataWritable worker1, V2WorkerMetadataWritable worker2) throws InvalidJobException, IOException {
        storeWorker(worker1);
        storeWorker(worker2);
    }

    @Override
    public void updateWorker(V2WorkerMetadataWritable mwmd) throws IOException {
        storeWorker(mwmd);
    }

    @Override
    public void archiveWorker(V2WorkerMetadataWritable mwmd) throws IOException {
        String workerId = getWorkerId(mwmd);
        workerMetadataStore.remove(workerId);
        archiveWorkerStore.put(workerId, mwmd);
    }

    @Override
    public List<V2WorkerMetadataWritable> getArchivedWorkers(String jobid) throws IOException {
        List<V2WorkerMetadataWritable> workers = new ArrayList<>();
        for (V2WorkerMetadataWritable archived : archiveWorkerStore.values()) {
            if (archived.getJobId().equals(jobid)) {
                workers.add(archived);
            }
        }
        return workers;
    }

    @Override
    public void storeNewNamedJob(NamedJob namedJob) throws JobNameAlreadyExistsException, IOException {
        namedJobsById.put(namedJob.getName(), namedJob);
    }

    @Override
    public void updateNamedJob(NamedJob namedJob) throws InvalidNamedJobException, IOException {
        namedJobsById.put(namedJob.getName(), namedJob);
    }

    @Override
    public boolean deleteNamedJob(String name) throws IOException {
        return namedJobsById.remove(name) != null;
    }

    @Override
    public void storeCompletedJobForNamedJob(String name, NamedJob.CompletedJob job) throws IOException {
        // no-op for now
    }

    @Override
    public void removeCompledtedJobForNamedJob(String name, String jobId) throws IOException {
        // no-op for now
    }

    @Override
    public V2JobMetadataWritable loadArchivedJob(String jobId) throws IOException {
        return archivedJobMetadataStore.get(jobId);
    }

    @Override
    public void shutdown() {
    }

    /**
     * Reload, with new stage
     */
    private V2JobMetadataWritable reloadWithStage(V2JobMetadataWritable job, V2StageMetadataWritable reloadedStage) {
        V2JobMetadataWritable newJob;
        try {
            String json = MAPPER.writeValueAsString(job);
            newJob = MAPPER.readValue(json, V2JobMetadataWritable.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Serialization problem", e);
        }
        newJob.addJobStageIfAbsent(reloadedStage);
        return newJob;
    }

    /**
     * Reset fields not persisted into store.
     */
    private V2StageMetadataWritable reloadStage(V2StageMetadataWritable stage) {
        try {
            String json = MAPPER.writeValueAsString(stage);
            return MAPPER.readValue(json, V2StageMetadataWritable.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Serialization problem", e);
        }
    }

    /**
     * Reset fields not persisted into store.
     */
    private V2WorkerMetadataWritable reloadWorker(V2WorkerMetadataWritable worker) {
        try {
            String json = MAPPER.writeValueAsString(worker);
            return MAPPER.readValue(json, V2WorkerMetadataWritable.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Serialization problem", e);
        }
    }

    static String getWorkerId(V2WorkerMetadataWritable worker) {
        return worker.getJobId() + '_' + worker.getWorkerNumber() + '_' + worker.getWorkerIndex();
    }

    static String getStageId(V2StageMetadataWritable msmd) {
        return msmd.getJobId() + '_' + msmd.getStageNum();
    }

    public static EmbeddedStorageProvider initializeFromFiles(String jobsFile, String jobStagesFile, String workersFile) {
        List<V2JobMetadataWritable> jobs;
        List<V2StageMetadataWritable> jobStages;
        List<V2WorkerMetadataWritable> workers;
        try {
            jobs = ObjectMappers.compactMapper().readValue(new File(jobsFile), JOB_METADATA_LIST);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot load file " + jobsFile, e);
        }
        try {
            jobStages = ObjectMappers.compactMapper().readValue(new File(jobStagesFile), JOB_STAGE_METADATA_LIST);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot load file " + jobStagesFile, e);
        }
        try {
            workers = ObjectMappers.compactMapper().readValue(new File(workersFile), WORKER_METADATA_LIST);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot load file " + workersFile, e);
        }
        Map<String, V2JobMetadataWritable> jobMap = jobs.stream().collect(Collectors.toMap(V2JobMetadataWritable::getJobId, Function.identity()));
        Map<String, V2StageMetadataWritable> stageMap = jobStages.stream()
                .filter(stage -> jobMap.containsKey(stage.getJobId()))
                .collect(Collectors.toMap(V2StageMetadataWritable::getJobId, Function.identity()));
        Map<String, V2WorkerMetadataWritable> workerMap = workers.stream()
                .filter(worker -> jobMap.containsKey(worker.getJobId()))
                .collect(Collectors.toMap(EmbeddedStorageProvider::getWorkerId, Function.identity()));
        return new EmbeddedStorageProvider(jobMap, stageMap, workerMap);
    }
}
