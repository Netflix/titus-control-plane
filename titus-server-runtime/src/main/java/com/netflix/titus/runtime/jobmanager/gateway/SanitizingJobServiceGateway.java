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
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdateWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobCapacityWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.common.util.rx.ReactorExt.toObservable;

public class SanitizingJobServiceGateway extends JobServiceGatewayDelegate {

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
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor;
        try {
            coreJobDescriptor = JobFunctions.filterOutGeneratedAttributes(
                    GrpcJobManagementModelConverters.toCoreJobDescriptor(jobDescriptor)
            );
        } catch (Exception e) {
            return Observable.error(TitusServiceException.invalidArgument(e));
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
                .onErrorResumeNext(throwable -> Observable.error(TitusServiceException.invalidArgument(throwable)))
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
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate, CallMetadata callMetadata) {
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
        final JobCapacityWithOptionalAttributes jobCapacityWithOptionalAttributes = jobCapacityUpdateWithOptionalAttributes.getJobCapacityWithOptionalAttributes();
        CapacityAttributes capacityAttributes = GrpcJobManagementModelConverters.toCoreCapacityAttributes(jobCapacityWithOptionalAttributes);
        Set<ValidationError> violations = entitySanitizer.validate(capacityAttributes);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }
        return delegate.updateJobCapacityWithOptionalAttributes(jobCapacityUpdateWithOptionalAttributes, callMetadata);
    }
}
