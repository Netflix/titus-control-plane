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

package com.netflix.titus.ext.aws.iam;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.titus.api.iam.model.IamRole;
import com.netflix.titus.api.json.ObjectMappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describes an AWS IAM Role Policy.
 */
public class AwsIamRole implements IamRole {
    private static final Logger logger = LoggerFactory.getLogger(AwsIamRole.class);

    private static final String assumeAction = "sts:AssumeRole";
    private static final String assumeEffect = "Allow";

    private final String roleId;
    private final String roleName;
    private final String resourceName;
    private final String assumePolicy;

    public AwsIamRole(String roleId, String roleName, String resourceName, String assumePolicy) {
        this.roleId = roleId;
        this.roleName = roleName;
        this.resourceName = resourceName;
        this.assumePolicy = assumePolicy;
    }

    @Override
    public String getRoleId() {
        return roleId;
    }

    @Override
    public String getRoleName() { return roleName; }

    @Override
    public String getResourceName() { return resourceName; }

    @Override
    public String getAssumePolicy() {
        return assumePolicy;
    }

    /**
     * Returns true of the provided AWS resource name can assume into this
     * role based on its policy.
     */
    @Override
    public Boolean canAssume(String assumeResourceName) {
        return canAssume(this.assumePolicy, assumeResourceName);
    }

    public static Boolean canAssume(String assumePolicy, String assumeResourceName) {
        try {
            // AWS provides us a URL encoded policy doc
            String assumePolicyJson = URLDecoder.decode(assumePolicy, StandardCharsets.UTF_8.toString());

            ObjectMapper objectMapper = ObjectMappers.defaultMapper();
            AwsAssumePolicy awsAssumePolicy = objectMapper.readValue(assumePolicyJson, AwsAssumePolicy.class);

            // Check if there is a policy statement that allows assumeRole for the provided resource
            for (AwsAssumeStatement statement : awsAssumePolicy.getStatements()) {
                if (statement.getAction().equals(assumeAction) &&
                        statement.getEffect().equals(assumeEffect) &&
                        null != statement.getPrincipal().getPrincipals()) {
                    for (String principalName : statement.getPrincipal().getPrincipals()) {
                        // Note this checks the resource name matches but does not check the account specifics.
                        // Consider a deeper check that matches the Agent account as well.
                        if (principalName.endsWith(assumeResourceName)) {
                            return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Unable to deserialize IAM role assumeRole policy {}: {}", assumePolicy, e.getMessage());
            return false;
        }
        return false;
    }

    @Override
    public String toString() {
        return "AwsIamRole{" +
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

        public AwsIamRole build() {
            return new AwsIamRole(roleId, roleName, resourceName, policyDoc);
        }
    }
}
