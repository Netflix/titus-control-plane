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

package io.netflix.titus.master.job.worker;

import java.io.IOException;

import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.master.store.V2JobMetadataWritable;
import io.netflix.titus.master.store.V2JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerNumberGenerator {
    private static final Logger logger = LoggerFactory.getLogger(WorkerNumberGenerator.class);
    private static final int IncrementStep = 50;
    private final String jobId;
    private V2JobStore jobStore = null;
    private int lastUsed;
    private int currLimit;
    private volatile boolean hasErrored = false;

    public WorkerNumberGenerator(String jobId) {
        this.jobId = jobId;
    }

    public void init(V2JobStore jobStore) {
        this.jobStore = jobStore;
        final V2JobMetadata job = this.jobStore.getActiveJob(jobId);
        lastUsed = job.getNextWorkerNumberToUse();
        currLimit = lastUsed;
    }

    private void advance() {
        final V2JobMetadata job = jobStore.getActiveJob(jobId);
        try (AutoCloseable l = job.obtainLock()) {
            currLimit += IncrementStep;
            ((V2JobMetadataWritable) job).setNextWorkerNumberToUse(currLimit);
            jobStore.storeJobNextWorkerNumber(jobId, currLimit);
            logger.info(jobId + " nextWorkerNumber set to : " + currLimit);
        } catch (InvalidJobException | IOException e) {
            hasErrored = true;
            throw new RuntimeException("Unexpected: " + e.getMessage());
        } catch (Exception e) {
            hasErrored = true;
            logger.warn("Unexpected exception locking job metadata object for jobid=" + jobId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Get the next unused worker number.
     * This call may lock the job metadata object to update storage of state that includes the last worker number used.
     * And, therefore, the caller must take care not to lock other job related objects. For example, if this call were
     * to be invoked from within a block that locked a worker metadata object, it could result in a deadlock since
     * generally the "outer" job object is locked first before locking the "inner" worker object, whenever both need to
     * be locked.
     * <p>
     * For performance reasosns, this object updates state in persistence every N calls made to this method.
     *
     * @return The next worker number to use for new workers
     */
    public int getNextWorkerNumber() {
        if (hasErrored) {
            throw new IllegalStateException("Unexpected: Invalid state likely due to getting/setting next worker number");
        }
        synchronized (this) {
            if (lastUsed == currLimit) {
                advance();
            }
            return ++lastUsed;
        }
    }
}
