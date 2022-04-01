/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.jobmanager.gateway;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.CapacityAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.admission.AdmissionSanitizer;
import com.netflix.titus.common.model.admission.AdmissionValidator;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdateWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobCapacityWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDisruptionBudgetUpdate;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.TaskAttributesUpdate;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.common.util.rx.ReactorExt.toObservable;

public class SanitizingJobServiceGateway extends JobServiceGatewayDelegate {

    private static final Logger logger = LoggerFactory.getLogger(SanitizingJobServiceGateway.class);

    private final JobServiceGateway delegate;
    private final EntitySanitizer entitySanitizer;
    private final AdmissionValidator<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> admissionValidator;
    private final AdmissionSanitizer<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> admissionSanitizer;

    public SanitizingJobServiceGateway(JobServiceGateway delegate,
                                       EntitySanitizer entitySanitizer,
                                       AdmissionValidator<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> validator,
                                       AdmissionSanitizer<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> sanitizer) {
        super(delegate);
        this.delegate = delegate;
        this.entitySanitizer = entitySanitizer;
        this.admissionValidator = validator;
        this.admissionSanitizer = sanitizer;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata) {
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptorUnfiltered;
        try {
            coreJobDescriptorUnfiltered = GrpcJobManagementModelConverters.toCoreJobDescriptor(jobDescriptor);
        } catch (Exception e) {
            return Observable.error(TitusServiceException.invalidArgument("Error creating core job descriptor: " + e.getMessage()));
        }

        com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor;
        try {
            coreJobDescriptor = JobFunctions.filterOutGeneratedAttributes(coreJobDescriptorUnfiltered);
        } catch (Exception e) {
            return Observable.error(TitusServiceException.invalidArgument("Error when filtering out generated attributes: " + e.getMessage()));
        }

        // basic entity validations based on class/field constraints
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor sanitizedCoreJobDescriptor = entitySanitizer.sanitize(coreJobDescriptor).orElse(coreJobDescriptor);
        Set<ValidationError> violations = entitySanitizer.validate(sanitizedCoreJobDescriptor);
        if (!violations.isEmpty()) {
            return Observable.error(TitusServiceException.invalidArgument(violations));
        }

        // validations that need external data
        return toObservable(admissionSanitizer.sanitizeAndApply(sanitizedCoreJobDescriptor)
                .switchIfEmpty(Mono.just(sanitizedCoreJobDescriptor)))
                .onErrorResumeNext(throwable -> {
                    logger.error("Sanitization error", throwable);
                    return Observable.error(
                            TitusServiceException.newBuilder(
                                            TitusServiceException.ErrorCode.INVALID_ARGUMENT,
                                            "Job sanitization error in TitusGateway: " + throwable.getMessage())
                                    .withCause(throwable)
                                    .build()
                    );
                })
                .flatMap(jd -> toObservable(admissionValidator.validate(jd))
                        .flatMap(errors -> {
                            // Only emit an error on HARD validation errors
                            errors = errors.stream().filter(ValidationError::isHard).collect(Collectors.toSet());
                            if (!errors.isEmpty()) {
                                return Observable.error(TitusServiceException.invalidJob(errors));
                            } else {
                                return Observable.just(jd);
                            }
                        })
                )
                .flatMap(jd -> delegate.createJob(GrpcJobManagementModelConverters.toGrpcJobDescriptor(jd), callMetadata));
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate, CallMetadata callMetadata) {
        return checkJobId(jobProcessesUpdate.getJobId())
                .map(Completable::error)
                .orElseGet(() -> delegate.updateJobProcesses(jobProcessesUpdate, callMetadata));
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate, CallMetadata callMetadata) {
        return checkJobId(statusUpdate.getId())
                .map(Completable::error)
                .orElseGet(() -> delegate.updateJobStatus(statusUpdate, callMetadata));
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(JobDisruptionBudgetUpdate request, CallMetadata callMetadata) {
        return checkJobId(request.getJobId())
                .map(Mono::<Void>error)
                .orElseGet(() -> delegate.updateJobDisruptionBudget(request, callMetadata));
    }

    @Override
    public Mono<Void> updateJobAttributes(JobAttributesUpdate request, CallMetadata callMetadata) {
        return checkJobId(request.getJobId())
                .map(Mono::<Void>error)
                .orElseGet(() -> delegate.updateJobAttributes(request, callMetadata));
    }

    @Override
    public Mono<Void> deleteJobAttributes(JobAttributesDeleteRequest request, CallMetadata callMetadata) {
        return checkJobId(request.getJobId())
                .map(Mono::<Void>error)
                .orElseGet(() -> delegate.deleteJobAttributes(request, callMetadata));
    }

    @Override
    public Observable<Job> findJob(String jobId, CallMetadata callMetadata) {
        return checkJobId(jobId)
                .map(Observable::<Job>error)
                .orElseGet(() -> findJob(jobId, callMetadata));
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId, CallMetadata callMetadata) {
        return checkJobId(jobId)
                .map(Observable::<JobChangeNotification>error)
                .orElseGet(() -> delegate.observeJob(jobId, callMetadata));
    }

    @Override
    public Completable killJob(String jobId, CallMetadata callMetadata) {
        return checkJobId(jobId)
                .map(Completable::error)
                .orElseGet(() -> delegate.killJob(jobId, callMetadata));
    }

    @Override
    public Observable<Task> findTask(String taskId, CallMetadata callMetadata) {
        return checkTaskId(taskId)
                .map(Observable::<Task>error)
                .orElseGet(() -> delegate.findTask(taskId, callMetadata));
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest, CallMetadata callMetadata) {
        return checkTaskId(taskKillRequest.getTaskId())
                .map(Completable::error)
                .orElseGet(() -> delegate.killTask(taskKillRequest, callMetadata));
    }

    @Override
    public Completable updateTaskAttributes(TaskAttributesUpdate attributesUpdate, CallMetadata callMetadata) {
        return checkTaskId(attributesUpdate.getTaskId())
                .map(Completable::error)
                .orElseGet(() -> delegate.updateTaskAttributes(attributesUpdate, callMetadata));
    }

    @Override
    public Completable deleteTaskAttributes(TaskAttributesDeleteRequest deleteRequest, CallMetadata callMetadata) {
        return checkTaskId(deleteRequest.getTaskId())
                .map(Completable::error)
                .orElseGet(() -> delegate.deleteTaskAttributes(deleteRequest, callMetadata));
    }

    @Override
    public Completable moveTask(TaskMoveRequest taskMoveRequest, CallMetadata callMetadata) {
        return checkTaskId(taskMoveRequest.getTaskId())
                .map(Completable::error)
                .orElseGet(() -> delegate.moveTask(taskMoveRequest, callMetadata));
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate, CallMetadata callMetadata) {
        Optional<TitusServiceException> badId = checkJobId(jobCapacityUpdate.getJobId());
        if (badId.isPresent()) {
            return Completable.error(badId.get());
        }
        Capacity newCapacity = GrpcJobManagementModelConverters.toCoreCapacity(jobCapacityUpdate.getCapacity());
        Set<ValidationError> violations = entitySanitizer.validate(newCapacity);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }
        return delegate.updateJobCapacity(jobCapacityUpdate, callMetadata);
    }

    @Override
    public Completable updateJobCapacityWithOptionalAttributes(JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes,
                                                               CallMetadata callMetadata) {
        Optional<TitusServiceException> badId = checkJobId(jobCapacityUpdateWithOptionalAttributes.getJobId());
        if (badId.isPresent()) {
            return Completable.error(badId.get());
        }
        final JobCapacityWithOptionalAttributes jobCapacityWithOptionalAttributes = jobCapacityUpdateWithOptionalAttributes.getJobCapacityWithOptionalAttributes();
        CapacityAttributes capacityAttributes = GrpcJobManagementModelConverters.toCoreCapacityAttributes(jobCapacityWithOptionalAttributes);
        Set<ValidationError> violations = entitySanitizer.validate(capacityAttributes);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }
        return delegate.updateJobCapacityWithOptionalAttributes(jobCapacityUpdateWithOptionalAttributes, callMetadata);
    }

    public static Optional<TitusServiceException> checkJobId(String jobId) {
        if (StringExt.isUUID(jobId)) {
            return Optional.empty();
        }
        return Optional.of(TitusServiceException.invalidArgument("Job id must be UUID string, but is: " + jobId));
    }

    public static Optional<TitusServiceException> checkTaskId(String taskId) {
        if (StringExt.isUUID(taskId)) {
            return Optional.empty();
        }
        return Optional.of(TitusServiceException.invalidArgument("Task id must be UUID string, but is: " + taskId));
    }
}
