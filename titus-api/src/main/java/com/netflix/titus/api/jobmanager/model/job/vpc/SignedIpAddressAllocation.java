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

package com.netflix.titus.api.jobmanager.model.job.vpc;

import java.util.Arrays;
import java.util.Objects;
import javax.validation.Valid;

import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;
import com.netflix.titus.common.model.sanitizer.FieldInvariant;

@ClassFieldsNotNull
public class SignedIpAddressAllocation {

    @Valid
    private final IpAddressAllocation ipAddressAllocation;

    @FieldInvariant(
            value = "@asserts.isBase64(value)",
            message = "Must be base64 encoded"
    )
    private final byte[] authoritativePublicKey;

    @FieldInvariant(
            value = "@asserts.isBase64(value)",
            message = "Must be base64 encoded"
    )
    private final byte[] hostPublicKey;

    @FieldInvariant(
            value = "@asserts.isBase64(value)",
            message = "Must be base64 encoded"
    )
    private final byte[] hostPublicKeySignature;

    @FieldInvariant(
            value = "@asserts.isBase64(value)",
            message = "Must be base64 encoded"
    )
    private final byte[] message;

    @FieldInvariant(
            value = "@asserts.isBase64(value)",
            message = "Must be base64 encoded"
    )
    private final byte[] messageSignature;

    public SignedIpAddressAllocation(IpAddressAllocation ipAddressAllocation,
                                     byte[] authoritativePublicKey,
                                     byte[] hostPublicKey,
                                     byte[] hostPublicKeySignature,
                                     byte[] message,
                                     byte[] messageSignature) {
        this.ipAddressAllocation = ipAddressAllocation;
        this.authoritativePublicKey = authoritativePublicKey;
        this.hostPublicKey = hostPublicKey;
        this.hostPublicKeySignature = hostPublicKeySignature;
        this.message = message;
        this.messageSignature = messageSignature;
    }

    public IpAddressAllocation getIpAddressAllocation() {
        return ipAddressAllocation;
    }

    public byte[] getAuthoritativePublicKey() {
        return authoritativePublicKey;
    }

    public byte[] getHostPublicKey() {
        return hostPublicKey;
    }

    public byte[] getHostPublicKeySignature() {
        return hostPublicKeySignature;
    }

    public byte[] getMessage() {
        return message;
    }

    public byte[] getMessageSignature() {
        return messageSignature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SignedIpAddressAllocation that = (SignedIpAddressAllocation) o;
        return ipAddressAllocation.equals(that.ipAddressAllocation) &&
                Arrays.equals(authoritativePublicKey, that.authoritativePublicKey) &&
                Arrays.equals(hostPublicKey, that.hostPublicKey) &&
                Arrays.equals(hostPublicKeySignature, that.hostPublicKeySignature) &&
                Arrays.equals(message, that.message) &&
                Arrays.equals(messageSignature, that.messageSignature);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(ipAddressAllocation);
        result = 31 * result + Arrays.hashCode(authoritativePublicKey);
        result = 31 * result + Arrays.hashCode(hostPublicKey);
        result = 31 * result + Arrays.hashCode(hostPublicKeySignature);
        result = 31 * result + Arrays.hashCode(message);
        result = 31 * result + Arrays.hashCode(messageSignature);
        return result;
    }

    @Override
    public String toString() {
        return "SignedIpAddressAllocation{" +
                "ipAddressAllocation=" + ipAddressAllocation +
                ", authoritativePublicKey=" + Arrays.toString(authoritativePublicKey) +
                ", hostPublicKey=" + Arrays.toString(hostPublicKey) +
                ", hostPublicKeySignature=" + Arrays.toString(hostPublicKeySignature) +
                ", message=" + Arrays.toString(message) +
                ", messageSignature=" + Arrays.toString(messageSignature) +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(SignedIpAddressAllocation copy) {
        Builder builder = new Builder();
        builder.ipAddressAllocation = copy.getIpAddressAllocation();
        builder.authoritativePublicKey = copy.getAuthoritativePublicKey();
        builder.hostPublicKey = copy.getHostPublicKey();
        builder.hostPublicKeySignature = copy.getHostPublicKeySignature();
        builder.message = copy.getMessage();
        builder.messageSignature = copy.getMessageSignature();
        return builder;
    }

    public static final class Builder {
        private IpAddressAllocation ipAddressAllocation;
        private byte[] authoritativePublicKey;
        private byte[] hostPublicKey;
        private byte[] hostPublicKeySignature;
        private byte[] message;
        private byte[] messageSignature;

        private Builder() {
        }

        public Builder withIpAddressAllocation(IpAddressAllocation val) {
            ipAddressAllocation = val;
            return this;
        }

        public Builder withAuthoritativePublicKey(byte[] val) {
            authoritativePublicKey = val;
            return this;
        }

        public Builder withHostPublicKey(byte[] val) {
            hostPublicKey = val;
            return this;
        }

        public Builder withHostPublicKeySignature(byte[] val) {
            hostPublicKeySignature = val;
            return this;
        }

        public Builder withMessage(byte[] val) {
            message = val;
            return this;
        }

        public Builder withMessageSignature(byte[] val) {
            messageSignature = val;
            return this;
        }

        public SignedIpAddressAllocation build() {
            return new SignedIpAddressAllocation(
                    ipAddressAllocation,
                    authoritativePublicKey,
                    hostPublicKey,
                    hostPublicKeySignature,
                    message,
                    messageSignature);
        }
    }
}
