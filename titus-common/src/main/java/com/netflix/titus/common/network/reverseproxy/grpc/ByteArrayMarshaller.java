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

package com.netflix.titus.common.network.reverseproxy.grpc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import io.grpc.MethodDescriptor;

/**
 * TODO Do not use byte array, but InputStream (see ServerInterceptors#useInputStreamMessages).
 */
class ByteArrayMarshaller implements MethodDescriptor.Marshaller<Object> {

    static ByteArrayMarshaller INSTANCE = new ByteArrayMarshaller();

    @Override
    public InputStream stream(Object value) {
        Preconditions.checkArgument(value instanceof byte[], "byte[] type expected");
        return new ByteArrayInputStream((byte[]) value);
    }

    @Override
    public Object parse(InputStream stream) {
        try {
            return ByteStreams.toByteArray(stream);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read from input stream", e);
        }
    }
}
