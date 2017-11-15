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

package io.netflix.titus.ext.aws;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.rx.ObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

import static io.netflix.titus.ext.aws.AwsInstanceCloudConnector.AWS_PAGE_MAX;
import static io.netflix.titus.ext.aws.AwsInstanceCloudConnector.TAG_ASG_NAME;
import static io.netflix.titus.ext.aws.AwsInstanceCloudConnector.TAG_TERMINATE;

/**
 * Removes all instances with tag {@link AwsInstanceCloudConnector#TAG_TERMINATE}. The fact that such instances exist
 * mean that their termination process failed.
 */
@Singleton
public class InstanceReaper {

    private final Logger logger = LoggerFactory.getLogger(InstanceReaper.class);

    private final AwsConfiguration configuration;
    private final AwsInstanceCloudConnector connector;
    private final Scheduler scheduler;

    private Subscription subscription;

    @Inject
    public InstanceReaper(AwsConfiguration configuration,
                          AwsInstanceCloudConnector connector) {
        this(configuration, connector, Schedulers.computation());
    }

    public InstanceReaper(AwsConfiguration configuration,
                          AwsInstanceCloudConnector connector,
                          Scheduler scheduler) {
        this.configuration = configuration;
        this.connector = connector;
        this.scheduler = scheduler;
    }

    @Activator
    public void enterActiveMode() {
        this.subscription = ObservableExt.schedule(
                doClean(),
                configuration.getReaperIntervalMs(), configuration.getReaperIntervalMs(), TimeUnit.MILLISECONDS,
                scheduler
        ).subscribe(
                next -> {
                    if (next.isPresent()) {
                        Throwable cause = next.get();
                        logger.warn("Instance termination execution terminated with an error: {}", cause.getMessage());
                        logger.debug(cause.getMessage(), cause);
                    } else {
                        logger.info("Instance termination execution succeeded");
                    }
                }
        );
    }

    @PreDestroy
    public void shutdown() {
        if (subscription != null) {
            subscription.unsubscribe();
        }
    }

    private Completable doClean() {
        return connector.getTaggedInstances(TAG_TERMINATE)
                .flatMapCompletable(vmsToTerminate -> {
                    logger.info("Found tagged instances: {}", vmsToTerminate.stream().map(Instance::getId).collect(Collectors.toList()));
                    List<String> ids = vmsToTerminate.stream()
                            .filter(this::canTerminate)
                            .limit(AWS_PAGE_MAX)
                            .map(Instance::getId)
                            .collect(Collectors.toList());
                    if (ids.isEmpty()) {
                        logger.info("No instance to remove");
                        return Completable.complete();
                    } else {
                        logger.info("Removing instances: {}", ids);
                        return connector.terminateDetachedInstances(ids);
                    }
                }).toCompletable();
    }

    private boolean canTerminate(Instance vm) {
        Instance.InstanceState state = vm.getInstanceState();
        if (state == Instance.InstanceState.Terminating || state == Instance.InstanceState.Terminated) {
            return false;
        }
        Map<String, String> attributes = vm.getAttributes();
        if (CollectionsExt.isNullOrEmpty(attributes)) {
            return false;
        }
        if (attributes.containsKey(TAG_ASG_NAME)) {
            return false;
        }
        return attributes.containsKey(TAG_TERMINATE);
    }
}
