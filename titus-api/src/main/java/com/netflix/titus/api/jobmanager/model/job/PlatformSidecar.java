package com.netflix.titus.api.jobmanager.model.job;

import java.util.Objects;
import javax.validation.Valid;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

public class PlatformSidecar {

    @Valid
    private final String name;

    @Valid
    private final String channel;

    @Valid
    private final Struct arguments;

    public PlatformSidecar(String name, String channel, Struct arguments) {
        this.name = name;
        this.channel = channel;
        this.arguments = arguments;
    }

    public String getName() {
        return name;
    }

    public String getChannel() {
        return channel;
    }

    public Struct getArguments() {
        return arguments;
    }

    public String getArgumentsJson() {
        try {
            return JsonFormat.printer().omittingInsignificantWhitespace().print(arguments);
        } catch (InvalidProtocolBufferException e) {
            return "BAD: " + e.getMessage();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlatformSidecar that = (PlatformSidecar) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(channel, that.channel) &&
                Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, channel, arguments);
    }

    @Override
    public String toString() {
        return "PlatformSidecar{" +
                "name='" + name + '\'' +
                ", channel='" + channel + '\'' +
                ", arguments=" + arguments +
                '}';
    }


    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String name;
        private String channel;
        private Struct arguments;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withChannel(String channel) {
            this.channel = channel;
            return this;
        }

        public Builder withArguments(Struct arguments) {
            this.arguments = arguments;
            return this;
        }

        public PlatformSidecar build() {
            return new PlatformSidecar(name, channel, arguments);
        }
    }

}
