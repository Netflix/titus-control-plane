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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.List;
import java.util.Map;
import javax.validation.constraints.Size;

import com.netflix.titus.common.model.sanitizer.CollectionInvariants;
import com.netflix.titus.common.model.sanitizer.FieldInvariant;
import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;
import com.netflix.titus.common.util.CollectionsExt;

import static com.netflix.titus.common.util.CollectionsExt.nonNull;

/**
 */
@ClassFieldsNotNull
public class SecurityProfile {

    private static final SecurityProfile EMPTY = newBuilder().build();

    @FieldInvariant(value = "@asserts.isValidSyntax(value)", message = "Syntactically invalid security group ids: #{value}")
    @Size(min = 1, max = 6, message = "Number of security groups must be between 1 and 6")
    private final List<String> securityGroups;

    @FieldInvariant(value = "@asserts.isValidIamRole(value)", message = "Syntactically invalid IAM role: #{value}")
    private final String iamRole;

    @CollectionInvariants
    private final Map<String, String> attributes;

    public SecurityProfile(List<String> securityGroups, String iamRole, Map<String, String> attributes) {
        this.securityGroups = CollectionsExt.nullableImmutableCopyOf(securityGroups);
        this.iamRole = iamRole;
        this.attributes = CollectionsExt.nullableImmutableCopyOf(attributes);
    }

    public List<String> getSecurityGroups() {
        return securityGroups;
    }

    public String getIamRole() {
        return iamRole;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SecurityProfile that = (SecurityProfile) o;

        if (securityGroups != null ? !securityGroups.equals(that.securityGroups) : that.securityGroups != null) {
            return false;
        }
        if (iamRole != null ? !iamRole.equals(that.iamRole) : that.iamRole != null) {
            return false;
        }
        return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
    }

    @Override
    public int hashCode() {
        int result = securityGroups != null ? securityGroups.hashCode() : 0;
        result = 31 * result + (iamRole != null ? iamRole.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SecurityProfile{" +
                "securityGroups=" + securityGroups +
                ", iamRole='" + iamRole + '\'' +
                ", attributes=" + attributes +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static SecurityProfile empty() {
        return EMPTY;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(SecurityProfile securityProfile) {
        return new Builder()
                .withSecurityGroups(securityProfile.getSecurityGroups())
                .withIamRole(securityProfile.getIamRole())
                .withAttributes(securityProfile.getAttributes());
    }

    public static final class Builder {
        private List<String> securityGroups;
        private String iamRole;
        private Map<String, String> attributes;

        private Builder() {
        }

        public Builder withSecurityGroups(List<String> securityGroups) {
            this.securityGroups = securityGroups;
            return this;
        }

        public Builder withIamRole(String iamRole) {
            this.iamRole = iamRole;
            return this;
        }

        public Builder withAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder but() {
            return newBuilder().withSecurityGroups(securityGroups).withIamRole(iamRole);
        }

        public SecurityProfile build() {
            return new SecurityProfile(nonNull(securityGroups), iamRole, nonNull(attributes));
        }
    }
}
