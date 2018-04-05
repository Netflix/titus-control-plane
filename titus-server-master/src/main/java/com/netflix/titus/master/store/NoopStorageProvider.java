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

package com.netflix.titus.master.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.netflix.titus.api.store.v2.InvalidJobException;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import rx.Observable;

public class NoopStorageProvider implements V2StorageProvider {
    @Override
    public void storeNewJob(V2JobMetadataWritable jobMetadata) throws JobAlreadyExistsException, IOException {
    }

    @Override
    public void updateJob(V2JobMetadataWritable jobMetadata) throws InvalidJobException, IOException {
    }

    @Override
    public void deleteJob(String jobId) throws InvalidJobException, IOException {
    }

    @Override
    public void archiveJob(String jobId) throws IOException {
    }

    @Override
    public void storeStage(V2StageMetadataWritable msmd) throws IOException {
    }

    @Override
    public void updateStage(V2StageMetadataWritable msmd) throws IOException {
    }

    @Override
    public void storeWorker(V2WorkerMetadataWritable workerMetadata) throws IOException {
    }

    @Override
    public void storeWorkers(String jobId, List<V2WorkerMetadataWritable> workers)
            throws IOException {
    }

    @Override
    public void storeAndUpdateWorkers(V2WorkerMetadataWritable worker1, V2WorkerMetadataWritable worker2) throws InvalidJobException, IOException {
    }

    @Override
    public void updateWorker(V2WorkerMetadataWritable mwmd) throws IOException {
    }

    @Override
    public List<V2JobMetadataWritable> initJobs() throws IOException {
        return new ArrayList<>();
    }

    @Override
    public Observable<V2JobMetadata> initArchivedJobs() {
        return Observable.empty();
    }

    @Override
    public List<NamedJob> initNamedJobs() throws IOException {
        return new ArrayList<>();
    }

    @Override
    public Observable<NamedJob.CompletedJob> initNamedJobCompletedJobs() throws IOException {
        return Observable.empty();
    }

    @Override
    public void archiveWorker(V2WorkerMetadataWritable mwmd) throws IOException {
    }

    @Override
    public List<V2WorkerMetadataWritable> getArchivedWorkers(String jobid) throws IOException {
        return new ArrayList<>();
    }

    @Override
    public void storeNewNamedJob(NamedJob namedJob) throws JobNameAlreadyExistsException, IOException {
    }

    @Override
    public void updateNamedJob(NamedJob namedJob) throws InvalidNamedJobException, IOException {
    }

    @Override
    public boolean deleteNamedJob(String name) throws IOException {
        return true;
    }

    @Override
    public void storeCompletedJobForNamedJob(String name, NamedJob.CompletedJob job) {
        return;
    }

    @Override
    public V2JobMetadataWritable loadArchivedJob(String jobId) throws IOException {
        return null;
    }

    @Override
    public void removeCompledtedJobForNamedJob(String name, String jobId) throws IOException {
        return;
    }

    @Override
    public void shutdown() {
    }
}
