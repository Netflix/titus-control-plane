package com.netflix.titus.common.network.reverseproxy.grpc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import io.grpc.MethodDescriptor;

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
