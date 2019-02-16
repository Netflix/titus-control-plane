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

package com.netflix.titus.ext.aws;

import com.netflix.titus.api.iam.model.IamRole;
import com.netflix.titus.ext.aws.iam.AwsIamUtil;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AwsIamUtilTest {
    /**
     * The AWS assume policy is provided by AWS in URL encoded format. The decoded JSON looks like:
     * {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"},{"Effect":"Allow","Principal":{"AWS":["AROAIBCLYUHWQPCHAZC34","arn:aws:iam::123456789012:role/myAssumableRole","AROAIBCLYUHWQPCHAZC34"]},"Action":"sts:AssumeRole"}]}
     */
    private static final String VALID_ASSUME_POLICY_DOC = "%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%7B%22Effect%22%3A%22Allow%22%2C%22Principal%22%3A%7B%22Service%22%3A%22ec2.amazonaws.com%22%7D%2C%22Action%22%3A%22sts%3AAssumeRole%22%7D%2C%7B%22Effect%22%3A%22Allow%22%2C%22Principal%22%3A%7B%22AWS%22%3A%5B%22AIDAJQABLZS4A3QDU576Q%22%2C%22arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2FmyAssumableRole%22%2C%22AIDAJQABLZS4A3QDU576Q%22%5D%7D%2C%22Action%22%3A%22sts%3AAssumeRole%22%7D%5D%7D";
    private static final String INVALID_ASSUME_POLICY_DOC = "%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%7B%22Effect%22%3A%22Allow%22%2C%22Principal%22%3A%7B%22Service%22%3A%22ec2.amazonaws.com%22%7D%2C%22Action%22%3A%22sts%3AAssumeRole%22%7D%2C%7B%22Effect%22%3A%22Allow%22%2C%22Principal%22%3A%7B%22AWS%22%3A%5B%22AIDAJQABLZS4A3QDU576Q%22%2C%22arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2FmyAssumableRole%22%2C%22AIDAJQABLZS4A3QDU576Q%22%5D%7D%2C%22Action%22%3A%22";
    private static final String ASSUMABLE_ROLE = "myAssumableRole";
    private static final String UNASSUMABLE_ROLE = "myUnassumableRole";

    private static final IamRole VALID_IAM_ROLE = IamRole.newBuilder()
            .withRoleId("AIDAJQABLZS4A3QDU576Q")
            .withRoleName("validRoleName")
            .withResourceName("arn:aws:iam::123456789012:role/validRoleName")
            .withPolicyDoc(VALID_ASSUME_POLICY_DOC)
            .build();
    private static final IamRole INVALID_IAM_ROLE = IamRole.newBuilder()
            .withRoleId("AIDAJQABLZS4A3QDU576Q")
            .withRoleName("invalidRoleName")
            .withResourceName("arn:aws:iam::123456789012:role/invalidRoleName")
            .withPolicyDoc(INVALID_ASSUME_POLICY_DOC)
            .build();

    @Test
    public void validateAssumablePolicyTest() {
        assertThat(AwsIamUtil.canAssume(VALID_IAM_ROLE, ASSUMABLE_ROLE)).isTrue();
    }

    @Test
    public void validateUnassumablePolicyTest() {
        assertThat(AwsIamUtil.canAssume(VALID_IAM_ROLE, UNASSUMABLE_ROLE)).isFalse();
    }

    @Test
    public void validateInvalidJsonPolicyTest() {
        assertThat(AwsIamUtil.canAssume(INVALID_IAM_ROLE, ASSUMABLE_ROLE)).isFalse();
    }
}
