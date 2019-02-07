/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.api.connector.cloud.noop;

import com.netflix.titus.api.iam.model.IamRole;

/**
 * Describes a generic IAM Role Policy. This class is intended for testing.
 */
public class NoopIamRole implements IamRole {

    private final static String noopString = "";

    @Override
    public String getRoleId() {
        return noopString;
    }

    @Override
    public String getRoleName() {
        return noopString;
    }

    @Override
    public String getResourceName() {
        return noopString;
    }

    @Override
    public String getAssumePolicy() {
        return noopString;
    }

    @Override
    public Boolean canAssume(String assumeResourceName) {
        return true;
    }
}
