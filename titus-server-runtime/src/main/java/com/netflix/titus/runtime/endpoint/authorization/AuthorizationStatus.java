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

package com.netflix.titus.runtime.endpoint.authorization;

public class AuthorizationStatus {

    private static final AuthorizationStatus SUCCESS = new AuthorizationStatus(true, "Access granted");

    private final boolean authorized;
    private final String reason;

    private AuthorizationStatus(boolean authorized, String reason) {
        this.authorized = authorized;
        this.reason = reason;
    }

    public boolean isAuthorized() {
        return authorized;
    }

    public String getReason() {
        return reason;
    }

    public static AuthorizationStatus success() {
        return SUCCESS;
    }

    public static AuthorizationStatus success(String reason) {
        return new AuthorizationStatus(true, reason);
    }

    public static AuthorizationStatus failure(String reason) {
        return new AuthorizationStatus(false, reason);
    }
}
