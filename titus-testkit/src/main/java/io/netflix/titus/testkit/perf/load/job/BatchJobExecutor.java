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

package io.netflix.titus.testkit.perf.load.job;

import java.util.function.Supplier;

import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.testkit.perf.load.ExecutionContext;
import rx.Observable;

public class BatchJobExecutor extends AbstractJobExecutor {

    public BatchJobExecutor(TitusJobSpec jobSpec,
                            Supplier<Observable<ActiveJobsMonitor.ActiveJobs>> activeJobsSupplier,
                            ExecutionContext context) {
        super(jobSpec, activeJobsSupplier, context);
    }

    public BatchJobExecutor(TitusJobSpec jobSpec,
                            ActiveJobsMonitor activeJobsMonitor,
                            ExecutionContext context) {
        super(jobSpec, activeJobsMonitor, context);
    }

    @Override
    public Observable<Void> terminateAndShrink(String taskId) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public Observable<Void> updateInstanceCount(int min, int desired, int max) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public Observable<Void> scaleUp(int delta) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public Observable<Void> scaleDown(int delta) {
        throw new IllegalStateException("Not supported");
    }
}
