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
import javax.inject.Named;

import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.CapacityAttributes;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdateWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobCapacityWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;

public class SanitizingJobServiceGateway extends JobServiceGatewayDelegate {

    private final JobServiceGateway delegate;
    private final EntitySanitizer entitySanitizer;

    public SanitizingJobServiceGateway(JobServiceGateway delegate,
                                       @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer) {
        super(delegate);
        this.delegate = delegate;
        this.entitySanitizer = entitySanitizer;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata) {
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor;
        try {
            coreJobDescriptor = V3GrpcModelConverters.toCoreJobDescriptor(jobDescriptor);
        } catch (Exception e) {
            return Observable.error(TitusServiceException.invalidArgument(e));
        }
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor sanitizedCoreJobDescriptor = entitySanitizer.sanitize(coreJobDescriptor).orElse(coreJobDescriptor);

        Set<ValidationError> violations = entitySanitizer.validate(sanitizedCoreJobDescriptor);
        if (!violations.isEmpty()) {
            return Observable.error(TitusServiceException.invalidArgument(violations));
        }

        JobDescriptor effectiveJobDescriptor = V3GrpcModelConverters.toGrpcJobDescriptor(sanitizedCoreJobDescriptor);

        return delegate.createJob(effectiveJobDescriptor, callMetadata);
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate) {
        Capacity newCapacity = V3GrpcModelConverters.toCoreCapacity(jobCapacityUpdate.getCapacity());
        Set<ValidationError> violations = entitySanitizer.validate(newCapacity);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }
        return delegate.updateJobCapacity(jobCapacityUpdate);
    }

    @Override
    public Completable updateJobCapacityWithOptionalAttributes(JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes) {
        final JobCapacityWithOptionalAttributes jobCapacityWithOptionalAttributes = jobCapacityUpdateWithOptionalAttributes.getJobCapacityWithOptionalAttributes();
        CapacityAttributes capacityAttributes = V3GrpcModelConverters.toCoreCapacityAttributes(jobCapacityWithOptionalAttributes);
        Set<ValidationError> violations = entitySanitizer.validate(capacityAttributes);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }
        return delegate.updateJobCapacityWithOptionalAttributes(jobCapacityUpdateWithOptionalAttributes);
    }
}
