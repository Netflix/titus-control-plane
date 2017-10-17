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

package io.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.NotificationCase;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.model.Page;
import io.netflix.titus.api.model.Pagination;
import io.netflix.titus.api.model.PaginationUtil;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.api.service.TitusServiceException.ErrorCode;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.endpoint.common.TaskSummary;
import io.netflix.titus.master.endpoint.grpc.GrpcEndpointConfiguration;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static io.netflix.titus.common.util.StringExt.safeTrim;

@Singleton
public class RoutingGrpcTitusServiceGateway implements GrpcTitusServiceGateway {

    public static final String NAME_V2_ENGINE_GATEWAY = "grpcToV2Engine";
    public static final String NAME_V3_ENGINE_GATEWAY = "grpcToV3Engine";
    private static final Logger logger = LoggerFactory.getLogger(RoutingGrpcTitusServiceGateway.class);
    private final GrpcTitusServiceGateway v2EngineGateway;
    private final GrpcTitusServiceGateway v3EngineGateway;
    private final GrpcEndpointConfiguration configuration;

    private volatile String whiteListRegExpStr;
    private volatile Optional<Pattern> whiteListRegExp;

    @Inject
    public RoutingGrpcTitusServiceGateway(
            @Named("grpcToV2Engine") GrpcTitusServiceGateway v2EngineGateway,
            @Named("grpcToV3Engine") GrpcTitusServiceGateway v3EngineGateway,
            GrpcEndpointConfiguration configuration) {
        this.v2EngineGateway = v2EngineGateway;
        this.v3EngineGateway = v3EngineGateway;
        this.configuration = configuration;

        this.whiteListRegExpStr = safeTrim(configuration.getV3EnabledApps()).intern();
        this.whiteListRegExp = whiteListRegExpStr.isEmpty()
                ? Optional.empty()
                : Optional.of(Pattern.compile(whiteListRegExpStr)); // Do not try/catch, as we let it fail on initialization
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor) {
        return isV3Enabled(jobDescriptor) ? v3EngineGateway.createJob(jobDescriptor) : v2EngineGateway.createJob(jobDescriptor);
    }

    @Override
    public Observable<Void> killJob(String user, String jobId) {
        return JobFunctions.isV2JobId(jobId) ? v2EngineGateway.killJob(user, jobId) : v3EngineGateway.killJob(user, jobId);
    }

    @Override
    public Observable<Job> findJobById(String jobId, boolean includeArchivedTasks, Set<TaskStatus.TaskState> taskStates) {
        return JobFunctions.isV2JobId(jobId)
                ? v2EngineGateway.findJobById(jobId, includeArchivedTasks, taskStates)
                : v3EngineGateway.findJobById(jobId, includeArchivedTasks, taskStates);
    }

    @Override
    public Pair<List<Job>, Pagination> findJobsByCriteria(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria, Optional<Page> pageOpt) {
        return findByCriteria(
                () -> v2EngineGateway.findJobsByCriteria(queryCriteria, Optional.of(Page.unlimited())),
                () -> v3EngineGateway.findJobsByCriteria(queryCriteria, Optional.of(Page.unlimited())),
                pageOpt
        );
    }

    @Override
    public Pair<List<Task>, Pagination> findTasksByCriteria(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria, Optional<Page> pageOpt) {
        return findByCriteria(
                () -> v2EngineGateway.findTasksByCriteria(queryCriteria, Optional.of(Page.unlimited())),
                () -> v3EngineGateway.findTasksByCriteria(queryCriteria, Optional.of(Page.unlimited())),
                pageOpt
        );
    }

    private <T> Pair<List<T>, Pagination> findByCriteria(Supplier<Pair<List<T>, Pagination>> v2Supplier,
                                                         Supplier<Pair<List<T>, Pagination>> v3Supplier,
                                                         Optional<Page> pageOpt) {
        if (!pageOpt.isPresent()) {
            throw TitusServiceException.newBuilder(ErrorCode.INVALID_ARGUMENT, "Page not provided").build();
        }
        Page page = pageOpt.get();

        // Get all matching entities, as we need totals anyway.
        Pair<List<T>, Pagination> v2Result = v2Supplier.get();
        Pair<List<T>, Pagination> v3Result = v3Supplier.get();

        List<T> allItems = new ArrayList<>(v2Result.getLeft());
        allItems.addAll(v3Result.getLeft());

        int offset = page.getPageSize() * page.getPageNumber();

        Pagination joinedPagination = new Pagination(
                page,
                allItems.size() > (offset + page.getPageSize()),
                PaginationUtil.numberOfPages(page, allItems.size()),
                allItems.size()
        );

        List<T> pageItems = offset >= allItems.size()
                ? Collections.emptyList()
                : allItems.subList(offset, Math.min(allItems.size(), offset + page.getPageSize()));
        return Pair.of(pageItems, joinedPagination);
    }

    @Override
    public Observable<Task> findTaskById(String taskId) {
        if (JobFunctions.isV2Task(taskId)) {
            return v2EngineGateway.findTaskById(taskId);
        }
        return v3EngineGateway.findTaskById(taskId).onErrorResumeNext(v2EngineGateway.findTaskById(taskId));
    }

    @Override
    public Observable<Void> resizeJob(String user, String jobId, int desired, int min, int max) {
        if (JobFunctions.isV2JobId(jobId)) {
            return v2EngineGateway.resizeJob(user, jobId, desired, min, max);
        }
        return v3EngineGateway.resizeJob(user, jobId, desired, min, max);
    }

    @Override
    public Observable<Void> changeJobInServiceStatus(String user, String serviceJobId, boolean inService) {
        if (JobFunctions.isV2JobId(serviceJobId)) {
            return v2EngineGateway.changeJobInServiceStatus(user, serviceJobId, inService);
        }
        return v3EngineGateway.changeJobInServiceStatus(user, serviceJobId, inService);
    }

    @Override
    public Observable<Void> killTask(String user, String taskId, boolean shrink) {
        if (JobFunctions.isV2Task(taskId)) {
            return v2EngineGateway.killTask(user, taskId, shrink);
        }
        return v3EngineGateway.killTask(user, taskId, shrink).onErrorResumeNext(
                ex -> v2EngineGateway.killTask(user, taskId, shrink)
        );
    }

    @Override
    public Observable<List<TaskSummary>> getTaskSummary() {
        throw new IllegalStateException("not implemented yet");
    }

    @Override
    public Observable<JobChangeNotification> observeJobs() {
        return v2EngineGateway.observeJobs().flatMap(event -> {
            if (event.getNotificationCase() == NotificationCase.SNAPSHOTEND) {
                return v3EngineGateway.observeJobs();
            }
            return Observable.just(event);
        });
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId) {
        return JobFunctions.isV2JobId(jobId) ? v2EngineGateway.observeJob(jobId) : v3EngineGateway.observeJob(jobId);
    }

    private boolean isV3Enabled(JobDescriptor jobDescriptor) {
        if (!whiteListRegExpStr.equals(configuration.getV3EnabledApps())) {
            resetWhiteListRegExp();
        }
        return whiteListRegExp.map(re -> re.matcher(jobDescriptor.getApplicationName()).matches()).orElse(false);
    }

    private void resetWhiteListRegExp() {
        this.whiteListRegExpStr = safeTrim(configuration.getV3EnabledApps());

        if (whiteListRegExpStr.isEmpty()) {
            this.whiteListRegExp = Optional.empty();
            return;
        }

        try {
            this.whiteListRegExp = Optional.of(Pattern.compile(whiteListRegExpStr));
        } catch (Exception e) {
            // If the new regexp is invalid, log this fact, and keep using the previous one.
            String activePattern = whiteListRegExp.map(Pattern::toString).orElse("<no_pattern>");
            logger.warn("Invalid V3 application white list regexp {}. Staying with the previous one: {}", whiteListRegExpStr, activePattern, e);
        }
    }
}
