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

package com.netflix.titus.runtime.common.json;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.Deserializers;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.netflix.titus.grpc.protogen.Image;
import com.netflix.titus.common.util.StringExt;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class ProtobufMessageObjectMapperTest {

    public static final String IMAGE_WRAPPER_JSON_STRING = "{\"image\":{\"name\":\"ubuntu\",\"tag\":\"latest\",\"digest\":\"\"}}";

    @Test
    public void testMessageSerialization() throws Exception {
        Image image = getImage();
        ObjectMapper objectMapper = getObjectMapper();
        String jsonString = objectMapper.writeValueAsString(image);
        String expectedJsonString = JsonFormat.printer()
                .includingDefaultValueFields()
                .print(image);
        Assertions.assertThat(jsonString).isEqualTo(expectedJsonString);
    }

    @Test
    public void testMessageDeserialization() throws Exception {
        Image expectedImage = getImage();
        String jsonString = JsonFormat.printer()
                .includingDefaultValueFields()
                .print(expectedImage);
        ObjectMapper objectMapper = getObjectMapper();
        Image image = objectMapper.readValue(jsonString, Image.class);
        Assertions.assertThat(image).isEqualTo(expectedImage);
    }

    @Test
    public void testStringSerialization() throws Exception {
        String text = "TestString";
        ObjectMapper objectMapper = getObjectMapper();
        String jsonString = objectMapper.writeValueAsString(text);

        Assertions.assertThat(jsonString).isEqualTo(StringExt.doubleQuotes(text));
    }

    @Test
    public void testStringDeserialization() throws Exception {
        String text = "TestString";
        ObjectMapper objectMapper = getObjectMapper();
        String jsonString = objectMapper.readValue(StringExt.doubleQuotes(text), String.class);

        Assertions.assertThat(jsonString).isEqualTo(text);
    }

    @Test
    public void testImageWrapperSerialization() throws Exception {
        ImageWrapper imageWrapper = new ImageWrapper(getImage());
        ObjectMapper objectMapper = getObjectMapper();
        String jsonString = objectMapper.writeValueAsString(imageWrapper)
                .replaceAll(" ", "")
                .replaceAll("\n", "");

        Assertions.assertThat(jsonString).isEqualTo(IMAGE_WRAPPER_JSON_STRING);
    }

    @Test
    public void testImageWrapperDeserialization() throws Exception {
        ImageWrapper expectedImageWrapper = new ImageWrapper(getImage());
        ObjectMapper objectMapper = getObjectMapper();
        ImageWrapper imageWrapper = objectMapper.readValue(IMAGE_WRAPPER_JSON_STRING, ImageWrapper.class);

        Assertions.assertThat(imageWrapper).isEqualTo(expectedImageWrapper);
    }

    private ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Serialization
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // Deserialization
        mapper.disable(SerializationFeature.INDENT_OUTPUT);

        SimpleDeserializers simpleDeserializers = new SimpleDeserializers();
        simpleDeserializers.addDeserializer(String.class, new TrimmingStringDeserializer());

        List<Deserializers> deserializersList = Arrays.asList(
                new AssignableFromDeserializers(Message.class, new ProtobufMessageDeserializer()),
                simpleDeserializers
        );
        CompositeDeserializers compositeDeserializers = new CompositeDeserializers(deserializersList);
        CustomDeserializerSimpleModule module = new CustomDeserializerSimpleModule(compositeDeserializers);
        module.addSerializer(Message.class, new ProtobufMessageSerializer());
        mapper.registerModule(module);

        return mapper;
    }

    private Image getImage() {
        return Image.newBuilder()
                .setName("ubuntu")
                .setTag("latest")
                .build();
    }

    private static class ImageWrapper {
        private final Image image;

        @JsonCreator
        ImageWrapper(@JsonProperty("image") Image image) {
            this.image = image;
        }

        public Image getImage() {
            return image;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ImageWrapper that = (ImageWrapper) o;

            return image != null ? image.equals(that.image) : that.image == null;
        }

        @Override
        public int hashCode() {
            return image != null ? image.hashCode() : 0;
        }
    }
}