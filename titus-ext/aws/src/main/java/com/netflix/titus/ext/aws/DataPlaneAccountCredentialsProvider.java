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

package com.netflix.titus.ext.aws;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsync;
import com.netflix.titus.common.util.StringExt;

@Singleton
public class DataPlaneAccountCredentialsProvider implements Provider<AWSCredentialsProvider> {

    public static final String NAME = "dataPlaneCredentials";

    private final AwsConfiguration configuration;
    private final AWSSecurityTokenServiceAsync stsClient;
    private final AWSCredentialsProvider defaultCredentialsProvider;

    @Inject
    public DataPlaneAccountCredentialsProvider(AwsConfiguration configuration,
                                               AWSSecurityTokenServiceAsync stsClient,
                                               AWSCredentialsProvider defaultCredentialsProvider) {
        this.configuration = configuration;
        this.stsClient = stsClient;
        this.defaultCredentialsProvider = defaultCredentialsProvider;
    }

    @Override
    public AWSCredentialsProvider get() {
        String roleArn = configuration.getDataPlaneRoleArn();
        if (StringExt.isEmpty(roleArn)) {
            return defaultCredentialsProvider;
        }

        String roleSessionName = configuration.getDataPlaneRoleSessionName();
        int roleSessionDurationSeconds = configuration.getDataPlaneRoleSessionDurationSeconds();

        return new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName)
                .withStsClient(stsClient)
                .withRoleSessionDurationSeconds(roleSessionDurationSeconds)
                .build();
    }
}
