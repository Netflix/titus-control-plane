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

public class AssignableFromDeserializers extends Deserializers.Base {
    private final Class<?> assignableFromClass;
    private final JsonDeserializer<?> deserializer;

    public AssignableFromDeserializers(Class<?> assignableFromClass, JsonDeserializer<?> deserializer) {
        this.assignableFromClass = assignableFromClass;
        this.deserializer = deserializer;
    }

    @Override
    public JsonDeserializer<?> findEnumDeserializer(Class<?> type, DeserializationConfig config, BeanDescription beanDesc) throws JsonMappingException {
        return getDeserializer(type);
    }

    @Override
    public JsonDeserializer<?> findTreeNodeDeserializer(Class<? extends JsonNode> nodeType, DeserializationConfig config,
                                                        BeanDescription beanDesc) throws JsonMappingException {
        return getDeserializer(nodeType);
    }

    @Override
    public JsonDeserializer<?> findReferenceDeserializer(ReferenceType refType, DeserializationConfig config, BeanDescription beanDesc,
                                                         TypeDeserializer contentTypeDeserializer, JsonDeserializer<?> contentDeserializer)
            throws JsonMappingException {
        return getDeserializer(refType.getRawClass());
    }

    @Override
    public JsonDeserializer<?> findBeanDeserializer(JavaType type, DeserializationConfig config, BeanDescription beanDesc) throws JsonMappingException {
        return getDeserializer(type.getRawClass());
    }

    @Override
    public JsonDeserializer<?> findArrayDeserializer(ArrayType type, DeserializationConfig config, BeanDescription beanDesc,
                                                     TypeDeserializer elementTypeDeserializer, JsonDeserializer<?> elementDeserializer)
            throws JsonMappingException {
        return getDeserializer(type.getRawClass());
    }

    @Override
    public JsonDeserializer<?> findCollectionDeserializer(CollectionType type, DeserializationConfig config, BeanDescription beanDesc,
                                                          TypeDeserializer elementTypeDeserializer, JsonDeserializer<?> elementDeserializer)
            throws JsonMappingException {
        return getDeserializer(type.getRawClass());
    }

    @Override
    public JsonDeserializer<?> findCollectionLikeDeserializer(CollectionLikeType type, DeserializationConfig config, BeanDescription beanDesc,
                                                              TypeDeserializer elementTypeDeserializer, JsonDeserializer<?> elementDeserializer)
            throws JsonMappingException {
        return getDeserializer(type.getRawClass());
    }

    @Override
    public JsonDeserializer<?> findMapDeserializer(MapType type, DeserializationConfig config, BeanDescription beanDesc, KeyDeserializer keyDeserializer,
                                                   TypeDeserializer elementTypeDeserializer, JsonDeserializer<?> elementDeserializer)
            throws JsonMappingException {
        return getDeserializer(type.getRawClass());
    }

    @Override
    public JsonDeserializer<?> findMapLikeDeserializer(MapLikeType type, DeserializationConfig config, BeanDescription beanDesc,
                                                       KeyDeserializer keyDeserializer, TypeDeserializer elementTypeDeserializer,
                                                       JsonDeserializer<?> elementDeserializer) throws JsonMappingException {
        return getDeserializer(type.getRawClass());
    }

    private JsonDeserializer<?> getDeserializer(Class<?> type) {
        if (assignableFromClass.isAssignableFrom(type)) {
            return deserializer;
        }
        return null;
    }
}
