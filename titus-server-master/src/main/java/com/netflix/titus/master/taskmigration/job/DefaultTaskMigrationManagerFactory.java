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

package com.netflix.titus.master.taskmigration.job;

import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.Lists;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.master.taskmigration.TaskMigrationDetails;
import com.netflix.titus.master.taskmigration.TaskMigrationManager;
import com.netflix.titus.master.taskmigration.TaskMigrationManagerFactory;
import com.netflix.titus.master.taskmigration.V3TaskMigrationDetails;

@Singleton
public class DefaultTaskMigrationManagerFactory implements TaskMigrationManagerFactory {
    private static final int PER_SECOND_INTERVAL = 1;

    private final ServiceJobTaskMigratorConfig config;
    private final TokenBucket terminateTokenBucket;

    @Inject
    public DefaultTaskMigrationManagerFactory(ServiceJobTaskMigratorConfig config) {
        this.config = config;
        this.terminateTokenBucket = Limiters.createFixedIntervalTokenBucket("DefaultMigrationTerminates", config.getTerminateTokenBucketCapacity(), 0,
                config.getTerminateTokenBucketRefillRatePerSecond(), PER_SECOND_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public TaskMigrationManager newTaskMigrationManager(TaskMigrationDetails taskMigrationDetails) {
        if (taskMigrationDetails instanceof V3TaskMigrationDetails) {
            V3TaskMigrationDetails v3TaskMigrationDetails = (V3TaskMigrationDetails) taskMigrationDetails;
            Job<?> job = v3TaskMigrationDetails.getJob();
            JobDescriptor.JobDescriptorExt extensions = job.getJobDescriptor().getExtensions();
            if (extensions instanceof ServiceJobExt) {
                ServiceJobExt serviceJobExt = (ServiceJobExt) extensions;
                com.netflix.titus.api.jobmanager.model.job.migration.MigrationPolicy migrationPolicy = serviceJobExt.getMigrationPolicy();
                if (migrationPolicy instanceof com.netflix.titus.api.jobmanager.model.job.migration.SelfManagedMigrationPolicy) {
                    return getSelfManagedMigrationManager(v3TaskMigrationDetails);
                }
            }
        }
        return new DefaultTaskMigrationManager(config, terminateTokenBucket);
    }

    private TaskMigrationManager getSelfManagedMigrationManager(TaskMigrationDetails taskMigrationDetails) {
        long migrationDeadline = taskMigrationDetails.getMigrationDeadline();
        long timeout = config.getSelfManagedTimeoutMs();
        if (migrationDeadline > 0) {
            timeout = Math.max(0, migrationDeadline - System.currentTimeMillis());
        }

        List<TaskMigrationManager> migrationManagers = Lists.newArrayList(
                new DelayTaskMigrationManager(timeout),
                new DefaultTaskMigrationManager(config, terminateTokenBucket)
        );
        return new CompositeTaskMigrationManager(migrationManagers);
    }
}
