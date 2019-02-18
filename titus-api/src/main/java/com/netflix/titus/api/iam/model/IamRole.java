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

package com.netflix.titus.api.iam.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes an IAM Role Policy.
 */
public class IamRole {
    private final String roleId;
    private final String roleName;
    private final String resourceName;
    private final String assumePolicy;

    @JsonCreator
    public IamRole(@JsonProperty("roleId")String roleId,
                   @JsonProperty("roleNmae")String roleName,
                   @JsonProperty("arn")String resourceName,
                   @JsonProperty("assumeRolePolicyDocument") String assumePolicy) {
        this.roleId = roleId;
        this.roleName = roleName;
        this.resourceName = resourceName;
        this.assumePolicy = assumePolicy;
    }

    public String getRoleId() {
        return roleId;
    }

    public String getRoleName() { return roleName; }

    public String getResourceName() { return resourceName; }

    public String getAssumePolicy() {
        return assumePolicy;
    }

    @Override
    public String toString() {
        return "IamRole{" +
                "roleId='" + roleId + '\'' +
                ", roleName='" + roleName + '\'' +
                ", resourceName='" + resourceName + '\'' +
                ", assumePolicy='" + assumePolicy + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String roleId;
        private String roleName;
        private String resourceName;
        private String policyDoc;

        private Builder() {}

        public Builder withRoleId(String roleId) {
            this.roleId = roleId;
            return this;
        }

        public Builder withRoleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        public Builder withResourceName(String resourceName) {
            this.resourceName = resourceName;
            return this;
        }

        public Builder withPolicyDoc(String policyDoc) {
            this.policyDoc = policyDoc;
            return this;
        }

        public IamRole build() {
            return new IamRole(roleId, roleName, resourceName, policyDoc);
        }
    }
}
