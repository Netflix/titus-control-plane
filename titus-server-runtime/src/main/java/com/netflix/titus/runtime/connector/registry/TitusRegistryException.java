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

package com.netflix.titus.runtime.connector.registry;

import org.springframework.http.HttpStatus;

/**
 * A custom {@link RuntimeException} implementation that indicates errors communicating with a container image registry.
 */
public class TitusRegistryException extends RuntimeException {

    public enum ErrorCode {
        INTERNAL,
        MISSING_HEADER,
        IMAGE_NOT_FOUND,
    }

    private final ErrorCode errorCode;
    private final String repository;
    private final String reference;

    public TitusRegistryException(ErrorCode errorCode, String repository, String reference, String message) {
        this(errorCode, repository, reference, message, new RuntimeException(message));
    }

    private TitusRegistryException(ErrorCode errorCode, String repository, String reference, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.repository = repository;
        this.reference = reference;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public String getRepository() {
        return repository;
    }

    public String getReference() {
        return reference;
    }

    public static TitusRegistryException imageNotFound(String repository, String reference) {
        return new TitusRegistryException(
                TitusRegistryException.ErrorCode.IMAGE_NOT_FOUND,
                repository,
                reference,
                String.format("Image %s:%s does not exist in registry", repository, reference)
        );
    }

    public static TitusRegistryException internalError(String repository, String reference, HttpStatus statusCode) {
        return new TitusRegistryException(TitusRegistryException.ErrorCode.INTERNAL,
                repository,
                reference,
                String.format("Cannot fetch image %s:%s metadata: statusCode=%s", repository, reference, statusCode));
    }

    public static TitusRegistryException headerMissing(String repository, String reference, String missingHeader) {
        return new TitusRegistryException(TitusRegistryException.ErrorCode.MISSING_HEADER,
                repository,
                reference,
                "Missing required header " + missingHeader);
    }
}
