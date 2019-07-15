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

package com.netflix.titus.api.json;

import java.util.List;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.deser.Deserializers;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.type.ArrayType;
import com.fasterxml.jackson.databind.type.CollectionLikeType;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapLikeType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.ReferenceType;

/**
 * Aggregates multiple {@link Deserializers} and iterates through them until a non-null value is returned.
 * Returns null if none of the values are non-null
 */
public class CompositeDeserializers extends Deserializers.Base {
    private final List<Deserializers> deserializersList;

    public CompositeDeserializers(List<Deserializers> deserializers) {
        this.deserializersList = deserializers;
    }

    @Override
    public JsonDeserializer<?> findEnumDeserializer(Class<?> type, DeserializationConfig config, BeanDescription beanDesc)
            throws JsonMappingException {
        for (Deserializers deserializers : deserializersList) {
            JsonDeserializer<?> deserializer = deserializers.findEnumDeserializer(type, config, beanDesc);
            if (deserializer != null) {
                return deserializer;
            }
        }
        return null;
    }

    @Override
    public JsonDeserializer<?> findTreeNodeDeserializer(Class<? extends JsonNode> nodeType, DeserializationConfig config,
                                                        BeanDescription beanDesc) throws JsonMappingException {
        for (Deserializers deserializers : deserializersList) {
            JsonDeserializer<?> deserializer = deserializers.findTreeNodeDeserializer(nodeType, config, beanDesc);
            if (deserializer != null) {
                return deserializer;
            }
        }
        return null;
    }

    @Override
    public JsonDeserializer<?> findReferenceDeserializer(ReferenceType refType, DeserializationConfig config,
                                                         BeanDescription beanDesc, TypeDeserializer contentTypeDeserializer,
                                                         JsonDeserializer<?> contentDeserializer) throws JsonMappingException {
        for (Deserializers deserializers : deserializersList) {
            JsonDeserializer<?> deserializer = deserializers.findReferenceDeserializer(refType, config, beanDesc, contentTypeDeserializer, contentDeserializer);
            if (deserializer != null) {
                return deserializer;
            }
        }
        return null;
    }

    @Override
    public JsonDeserializer<?> findBeanDeserializer(JavaType type, DeserializationConfig config, BeanDescription beanDesc)
            throws JsonMappingException {
        for (Deserializers deserializers : deserializersList) {
            JsonDeserializer<?> deserializer = deserializers.findBeanDeserializer(type, config, beanDesc);
            if (deserializer != null) {
                return deserializer;
            }
        }
        return null;
    }

    @Override
    public JsonDeserializer<?> findArrayDeserializer(ArrayType type, DeserializationConfig config, BeanDescription beanDesc,
                                                     TypeDeserializer elementTypeDeserializer, JsonDeserializer<?> elementDeserializer)
            throws JsonMappingException {
        for (Deserializers deserializers : deserializersList) {
            JsonDeserializer<?> deserializer = deserializers.findArrayDeserializer(type, config, beanDesc, elementTypeDeserializer, elementDeserializer);
            if (deserializer != null) {
                return deserializer;
            }
        }
        return null;
    }

    @Override
    public JsonDeserializer<?> findCollectionDeserializer(CollectionType type, DeserializationConfig config, BeanDescription beanDesc,
                                                          TypeDeserializer elementTypeDeserializer, JsonDeserializer<?> elementDeserializer)
            throws JsonMappingException {
        for (Deserializers deserializers : deserializersList) {
            JsonDeserializer<?> deserializer = deserializers.findCollectionDeserializer(type, config, beanDesc, elementTypeDeserializer, elementDeserializer);
            if (deserializer != null) {
                return deserializer;
            }
        }
        return null;
    }

    @Override
    public JsonDeserializer<?> findCollectionLikeDeserializer(CollectionLikeType type, DeserializationConfig config,
                                                              BeanDescription beanDesc, TypeDeserializer elementTypeDeserializer,
                                                              JsonDeserializer<?> elementDeserializer) throws JsonMappingException {
        for (Deserializers deserializers : deserializersList) {
            JsonDeserializer<?> deserializer = deserializers.findCollectionLikeDeserializer(type, config, beanDesc, elementTypeDeserializer, elementDeserializer);
            if (deserializer != null) {
                return deserializer;
            }
        }
        return null;
    }

    @Override
    public JsonDeserializer<?> findMapDeserializer(MapType type, DeserializationConfig config, BeanDescription beanDesc,
                                                   KeyDeserializer keyDeserializer, TypeDeserializer elementTypeDeserializer,
                                                   JsonDeserializer<?> elementDeserializer) throws JsonMappingException {
        for (Deserializers deserializers : deserializersList) {
            JsonDeserializer<?> deserializer = deserializers.findMapDeserializer(type, config, beanDesc, keyDeserializer, elementTypeDeserializer, elementDeserializer);
            if (deserializer != null) {
                return deserializer;
            }
        }
        return null;
    }

    @Override
    public JsonDeserializer<?> findMapLikeDeserializer(MapLikeType type, DeserializationConfig config, BeanDescription beanDesc,
                                                       KeyDeserializer keyDeserializer, TypeDeserializer elementTypeDeserializer,
                                                       JsonDeserializer<?> elementDeserializer) throws JsonMappingException {
        for (Deserializers deserializers : deserializersList) {
            JsonDeserializer<?> deserializer = deserializers.findMapLikeDeserializer(type, config, beanDesc, keyDeserializer, elementTypeDeserializer, elementDeserializer);
            if (deserializer != null) {
                return deserializer;
            }
        }
        return null;
    }
}
