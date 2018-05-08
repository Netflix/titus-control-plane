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

package com.netflix.titus.common.framework.reconciler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import com.netflix.titus.common.util.tuple.Pair;

/**
 * Composite entity hierarchy. The parent-child association runs from parent to child only. {@link EntityHolder} instances
 * are immutable, thus each change produces a new version of an entity. Also each child update requires update of a parent
 * entity, when the reference to the child changes (a new version is created).
 */
public class EntityHolder {

    private final String id;
    private final Object entity;

    private final List<EntityHolder> children;
    private final Map<String, EntityHolder> childrenById;
    private final Map<String, Object> attributes;

    private EntityHolder(String id, Object entity, Map<String, EntityHolder> childrenById, Map<String, Object> attributes) {
        this.id = id;
        this.entity = entity;
        this.childrenById = childrenById;
        this.children = new ArrayList<>(childrenById.values());
        this.attributes = attributes;
    }

    public String getId() {
        return id;
    }

    public <E> E getEntity() {
        return (E) entity;
    }

    public List<EntityHolder> getChildren() {
        return children;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Optional<EntityHolder> findById(String requestedId) {
        if (this.id.equals(requestedId)) {
            return Optional.of(this);
        }
        return findChildById(requestedId);
    }

    public Optional<EntityHolder> findChildById(String childId) {
        if (children.isEmpty()) {
            return Optional.empty();
        }
        EntityHolder entityHolder = childrenById.get(childId);
        if (entityHolder != null) {
            return Optional.of(entityHolder);
        }
        for (EntityHolder child : children) {
            Optional<EntityHolder> result = child.findChildById(childId);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    public EntityHolder addChild(EntityHolder child) {
        Map<String, EntityHolder> newChildrenById = new HashMap<>(childrenById);
        newChildrenById.put(child.getId(), child);
        return new EntityHolder(id, entity, newChildrenById, attributes);
    }

    public Pair<EntityHolder, Optional<EntityHolder>> removeChild(String id) {
        if (!childrenById.containsKey(id)) {
            return Pair.of(this, Optional.empty());
        }
        Map<String, EntityHolder> newChildrenById = new HashMap<>(childrenById);
        EntityHolder removedChild = newChildrenById.remove(id);

        EntityHolder newRoot = new EntityHolder(this.id, this.entity, newChildrenById, this.attributes);
        return Pair.of(newRoot, Optional.of(removedChild));
    }

    public EntityHolder addTag(String tagName, Object tagValue) {
        Map<String, Object> newTags = new HashMap<>(attributes);
        newTags.put(tagName, tagValue);
        return new EntityHolder(id, entity, childrenById, newTags);
    }

    public EntityHolder removeTag(String tagName) {
        if (!attributes.containsKey(tagName)) {
            return this;
        }
        Map<String, Object> newTags = new HashMap<>(attributes);
        newTags.remove(tagName);
        return new EntityHolder(id, entity, childrenById, newTags);
    }

    public <E> EntityHolder setEntity(E entity) {
        return new EntityHolder(id, entity, childrenById, attributes);
    }

    public void visit(Consumer<EntityHolder> visitor) {
        visitor.accept(this);
        children.forEach(c -> c.visit(visitor));
    }

    public static <E> EntityHolder newRoot(String id, E entity) {
        return new EntityHolder(id, entity, Collections.emptyMap(), Collections.emptyMap());
    }
}
