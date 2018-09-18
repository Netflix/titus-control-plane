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

package com.netflix.titus.api.service;

import com.netflix.titus.common.model.validator.ValidationError;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolation;

import static java.lang.String.format;

/**
 * A custom runtime exception that indicates an error in the service layer and will propagate to transport layer.
 */
public class TitusServiceException extends RuntimeException {

    public enum ErrorCode {
        NOT_LEADER,
        NOT_READY,
        INTERNAL,
        NO_CALLER_ID,
        JOB_NOT_FOUND,
        JOB_UPDATE_NOT_ALLOWED,
        UNSUPPORTED_JOB_TYPE,
        TASK_NOT_FOUND,
        NOT_SUPPORTED,
        UNIMPLEMENTED,
        UNEXPECTED,
        INVALID_PAGE_OFFSET,
        INVALID_ARGUMENT,
        CELL_NOT_FOUND,
        INVALID_JOB
    }

    private final ErrorCode errorCode;
    private final Set<? extends ValidationError> validationErrors;
    private final Optional<String> leaderAddress;

    private TitusServiceException(TitusServiceExceptionBuilder builder) {
        super(builder.message, builder.cause);
        this.errorCode = builder.errorCode;
        this.validationErrors = builder.validationErrors;
        this.leaderAddress = builder.leaderAddress;
    }

    public static TitusServiceExceptionBuilder newBuilder(ErrorCode errorCode, String message) {
        return new TitusServiceExceptionBuilder(errorCode, message);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public Set<? extends ValidationError> getValidationErrors() {
        return validationErrors;
    }

    public Optional<String> getLeaderAddress() {
        return leaderAddress;
    }

    public static final class TitusServiceExceptionBuilder {
        ErrorCode errorCode;
        String message;
        Throwable cause;
        Set<? extends ValidationError> validationErrors;
        Optional<String> leaderAddress;

        private TitusServiceExceptionBuilder(ErrorCode errorCode, String message) {
            this.errorCode = errorCode;
            this.message = message;
        }

        public TitusServiceExceptionBuilder withCause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        public TitusServiceExceptionBuilder withValidationErrors(Set<? extends ValidationError> constraintViolations) {
            this.validationErrors = constraintViolations;
            return this;
        }

        public TitusServiceExceptionBuilder withLeaderAddress(String leaderAddress) {
            this.leaderAddress = Optional.ofNullable(leaderAddress);
            return this;
        }

        public TitusServiceException build() {
            if (this.validationErrors == null) {
                this.validationErrors = Collections.emptySet();
            }
            if (this.leaderAddress == null) {
                this.leaderAddress = Optional.empty();
            }
            if (errorCode == ErrorCode.NOT_LEADER && !this.leaderAddress.isPresent()) {
                throw new IllegalArgumentException("Leader address must be present when specifying not leader code");
            }
            return new TitusServiceException(this);
        }
    }

    public static TitusServiceException jobNotFound(String jobId) {
        return jobNotFound(jobId, null);
    }

    public static TitusServiceException jobNotFound(String jobId, Throwable cause) {
        return TitusServiceException.newBuilder(ErrorCode.JOB_NOT_FOUND, format("Job id %s not found", jobId))
                .withCause(cause)
                .build();
    }

    public static TitusServiceException taskNotFound(String taskId) {
        return taskNotFound(taskId, null);
    }

    public static TitusServiceException taskNotFound(String taskId, Throwable cause) {
        return TitusServiceException.newBuilder(ErrorCode.TASK_NOT_FOUND, format("Task id %s not found", taskId))
                .withCause(cause)
                .build();
    }

    public static TitusServiceException invalidArgument(String message) {
        return TitusServiceException.newBuilder(ErrorCode.INVALID_ARGUMENT, message).build();
    }

    public static TitusServiceException invalidArgument(Throwable e) {
        return TitusServiceException.newBuilder(ErrorCode.INVALID_ARGUMENT, e.getMessage()).withCause(e).build();
    }

    public static TitusServiceException invalidArgument(Set<? extends ValidationError> validationErrors) {
        String errors = validationErrors.stream()
                .map(err -> String.format("{%s}", err))
                .collect(Collectors.joining(", "));
        String errMsg = String.format("Invalid Argument: %s", errors);
        return TitusServiceException.newBuilder(ErrorCode.INVALID_ARGUMENT, errMsg)
                .withValidationErrors(validationErrors)
                .build();
    }

    /**
     * Creates a {@link TitusServiceException} encapsulating {@link ValidationError}s.
     *
     * @param validationErrors The errors to be encapsulated by the exception
     * @return A {@link TitusServiceException} encapsulating the appropriate errors.
     */
    public static TitusServiceException invalidJob(Set<ValidationError> validationErrors) {
        String errors = validationErrors.stream()
                .map(err -> String.format("{%s}", err))
                .collect(Collectors.joining(", "));
        String errMsg = String.format("Invalid Job: %s", errors);

        return TitusServiceException.newBuilder(ErrorCode.INVALID_JOB, errMsg).build();
    }

    public static TitusServiceException unexpected(String message, Object... args) {
        return unexpected(null, message, args);
    }

    public static TitusServiceException unexpected(Throwable cause, String message, Object... args) {
        return TitusServiceException.newBuilder(ErrorCode.UNEXPECTED, format(message, args))
                .withCause(cause)
                .build();
    }

    public static TitusServiceException notSupported() {
        return TitusServiceException.newBuilder(ErrorCode.NOT_SUPPORTED, "Not supported").build();
    }

    public static TitusServiceException unimplemented() {
        return TitusServiceException.newBuilder(ErrorCode.UNIMPLEMENTED, "Not implemented").build();
    }

    public static TitusServiceException notLeader(String leaderAddress) {
        return TitusServiceException.newBuilder(ErrorCode.NOT_LEADER, format("Not a leader node. Current leader is: %s", leaderAddress))
                .withLeaderAddress(leaderAddress)
                .build();
    }

    public static TitusServiceException unknownLeader() {
        return TitusServiceException.newBuilder(ErrorCode.NOT_LEADER, "Not a leader node")
                .build();
    }

    public static TitusServiceException notReady() {
        return TitusServiceException.newBuilder(ErrorCode.NOT_READY, "Leader not ready").build();
    }

    public static TitusServiceException noCallerId() {
        return TitusServiceException.newBuilder(ErrorCode.NO_CALLER_ID, "Caller's id not found").build();
    }

    public static TitusServiceException cellNotFound(String routeKey) {
        return TitusServiceException.newBuilder(ErrorCode.CELL_NOT_FOUND, format("Could not find routable Titus Cell for route key %s", routeKey)).build();
    }
}
