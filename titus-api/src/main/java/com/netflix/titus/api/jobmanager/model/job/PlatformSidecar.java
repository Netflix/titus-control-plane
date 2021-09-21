package com.netflix.titus.api.jobmanager.model.job;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import javax.validation.Valid;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

public class PlatformSidecar {

    @Valid
    private final String name;

    @Valid
    private final String channel;

    @Valid
    @JsonSerialize(using = ProtoStructSerializer.class)
    @JsonDeserialize(using = ProtoStructDeserializer.class)
//    @JsonSerialize(using = MyMessageSerializer.class)
//    @JsonDeserialize(using = MyMessageDeserializer.class, as = Struct.class, keyAs = String.class, contentAs = Value.class)
//    @JsonSerialize(as = Struct.class, keyAs = String.class, contentAs = Value.class)
//    @JsonDeserialize(as = Struct.class, keyAs = String.class, contentAs = Value.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public final Struct arguments;

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

class ProtoStructSerializer extends JsonSerializer<Struct> {
    @Override
    public void serialize(Struct message, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeRawValue(JsonFormat.printer().print(message));
    }
}

class ProtoStructDeserializer extends JsonDeserializer<Struct> {
    @Override
    public Struct deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        Struct.Builder argsBuilder = Struct.newBuilder();
        Map m = p.readValueAs(Map.class);
        JsonFormat.parser().merge(m.toString(), argsBuilder);
        return argsBuilder.build();
    }
}