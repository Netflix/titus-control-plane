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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes an AWS assume policy statement.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AwsAssumeStatement {
    private final String effect;
    private final AwsAssumePrincipal principal;
    private final String action;

    @JsonCreator
    public AwsAssumeStatement(@JsonProperty("Effect") String effect,
                              @JsonProperty("Principal") AwsAssumePrincipal principal,
                              @JsonProperty("Action") String action) {
        this.effect = effect;
        this.principal = principal;
        this.action = action;
    }

    public String getEffect() {
        return effect;
    }

    public AwsAssumePrincipal getPrincipal() {
        return principal;
    }

    public String getAction() {
        return action;
    }

    @Override
    public String toString() {
        return "AwsAssumeStatement{" +
                "effect='" + effect + '\'' +
                ", principal=" + principal +
                ", action='" + action + '\'' +
                '}';
    }
}
