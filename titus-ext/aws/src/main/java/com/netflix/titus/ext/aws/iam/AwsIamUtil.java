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

public class AwsIamUtil {
    private static final Logger logger = LoggerFactory.getLogger(AwsIamUtil.class);

    private static final String ASSUME_ACTION = "sts:AssumeRole";
    private static final String ASSUME_EFFECT = "Allow";

    /**
     * Helper method to deserialize an IAM Role's assume policy and verify if
     * another role can assume into it.
     */
    public static boolean canAssume(IamRole iamRole, String assumeResourceName) {
        try {
            // AWS provides us a URL encoded policy doc
            String assumePolicyJson = URLDecoder.decode(iamRole.getAssumePolicy(), StandardCharsets.UTF_8.toString());

            ObjectMapper objectMapper = ObjectMappers.defaultMapper();
            AwsAssumePolicy awsAssumePolicy = objectMapper.readValue(assumePolicyJson, AwsAssumePolicy.class);

            // Check if there is a policy statement that allows assumeRole for the provided resource
            for (AwsAssumeStatement statement : awsAssumePolicy.getStatements()) {
                if (statementCanAssume(statement, assumeResourceName)) {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.warn("Unable to deserialize IAM role assumeRole policy {}: {}", iamRole.getAssumePolicy(), e.getMessage());
            return false;
        }
        return false;
    }

    private static boolean statementCanAssume(AwsAssumeStatement statement, String assumeResourceName) {
        if (statement.getAction().equals(ASSUME_ACTION) &&
                statement.getEffect().equals(ASSUME_EFFECT) &&
                !statement.getPrincipal().getPrincipals().isEmpty()) {
            for (String principalName : statement.getPrincipal().getPrincipals()) {
                // Note this checks the resource name matches but does not check the account specifics.
                // Consider a deeper check that matches the Agent account as well.
                if (principalName.endsWith(assumeResourceName)) {
                    return true;
                }
            }
        }
        return false;
    }
}
