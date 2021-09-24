package com.netflix.titus.api.jobmanager.model.job;

import java.util.Objects;
import javax.validation.Valid;


public class PlatformSidecar {

    @Valid
    private final String name;

    @Valid
    private final String channel;

    @Valid
    public final String arguments;

    public PlatformSidecar(String name, String channel, String arguments) {
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

    public String getArguments() {
        return arguments;
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
        private String arguments;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withChannel(String channel) {
            this.channel = channel;
            return this;
        }

        public Builder withArguments(String arguments) {
            this.arguments = arguments;
            return this;
        }

        public PlatformSidecar build() {
            return new PlatformSidecar(name, channel, arguments);
        }
    }

}