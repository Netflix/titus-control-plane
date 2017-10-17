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

package io.netflix.titus.common.network.http;


/**
 * Commonly used status codes defined by HTTP, see
 * {@link <a href="https://en.wikipedia.org/wiki/List_of_HTTP_status_codes">List of Http status codes</a>}
 * for the complete list.
 */
public enum StatusCode {
    /**
     * 200 OK
     */
    OK(200, "OK", false),
    /**
     * 201 Created
     */
    CREATED(201, "Created", false),
    /**
     * 202 Accepted
     */
    ACCEPTED(202, "Accepted", false),
    /**
     * 204 No Content
     */
    NO_CONTENT(204, "No Content", false),
    /**
     * 301 Moved Permanently
     */
    MOVED_PERMANENTLY(301, "Moved Permanently", false),
    /**
     * 303 See Other
     */
    SEE_OTHER(303, "See Other", false),
    /**
     * 304 Not Modified
     */
    NOT_MODIFIED(304, "Not Modified", false),
    /**
     * 307 Temporary Redirect
     */
    TEMPORARY_REDIRECT(307, "Temporary Redirect", false),
    /**
     * 400 Bad Request
     */
    BAD_REQUEST(400, "Bad Request", false),
    /**
     * 401 Unauthorized
     */
    UNAUTHORIZED(401, "Unauthorized", false),
    /**
     * 403 Forbidden
     */
    FORBIDDEN(403, "Forbidden", false),
    /**
     * 404 Not Found
     */
    NOT_FOUND(404, "Not Found", false),
    /**
     * 406 Not Acceptable
     */
    NOT_ACCEPTABLE(406, "Not Acceptable", false),
    /**
     * 409 Conflict
     */
    CONFLICT(409, "Conflict", false),
    /**
     * 410 Gone
     */
    GONE(410, "Gone", false),
    /**
     * 412 Precondition Failed
     */
    PRECONDITION_FAILED(412, "Precondition Failed", false),
    /**
     * 415 Unsupported Media Type
     */
    UNSUPPORTED_MEDIA_TYPE(415, "Unsupported Media Type", false),
    /**
     * 429 Too Many Requests
     */
    TOO_MANY_REQUESTS(429, "Too Many Requests", true),
    /**
     * 500 Internal Server Error
     */
    INTERNAL_SERVER_ERROR(500, "Internal Server Error", true),
    /**
     * 503 Service Unavailable
     */
    SERVICE_UNAVAILABLE(503, "Service Unavailable", true);

    private final int code;
    private final String description;
    private StatusClass statusClass;
    private boolean retryable;

    /**
     * An enumeration representing the class of status code. Family is used
     * here since class is overloaded in Java.
     */
    public enum StatusClass {
        INFORMATIONAL, SUCCESSFUL, REDIRECTION, CLIENT_ERROR, SERVER_ERROR, OTHER
    }

    StatusCode(final int code, final String description, final boolean retryable) {
        this.code = code;
        this.description = description;
        this.retryable = retryable;
        switch (code / 100) {
            case 1:
                this.statusClass = StatusClass.INFORMATIONAL;
                break;
            case 2:
                this.statusClass = StatusClass.SUCCESSFUL;
                break;
            case 3:
                this.statusClass = StatusClass.REDIRECTION;
                break;
            case 4:
                this.statusClass = StatusClass.CLIENT_ERROR;
                break;
            case 5:
                this.statusClass = StatusClass.SERVER_ERROR;
                break;
            default:
                this.statusClass = StatusClass.OTHER;
                break;
        }
    }

    /**
     * Get the class of status code
     *
     * @return the class of status code
     */
    public StatusClass getStatusClass() {
        return statusClass;
    }

    /**
     * Get the associated status code
     *
     * @return the status code
     */
    public int getCode() {
        return code;
    }

    /**
     * Get the description
     *
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Whether or not this status code should be retried
     *
     * @return true if this status code should be retried
     */
    public boolean isRetryable() {
        return retryable;
    }

    /**
     * The string representation
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        return "[" + code + "] " + description;
    }

    /**
     * Convert a numerical status code into the corresponding Status
     *
     * @param code the numerical status code
     * @return the matching Status or null is no matching Status is defined
     */
    public static StatusCode fromCode(final int code) {
        for (StatusCode s : StatusCode.values()) {
            if (s.code == code) {
                return s;
            }
        }
        return null;
    }
}
