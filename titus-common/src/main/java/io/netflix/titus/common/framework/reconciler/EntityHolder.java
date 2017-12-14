/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.framework.reconciler;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import io.netflix.titus.common.util.tuple.Pair;

/**
 * Composite entity hierarchy. The parent-child association runs from parent to child only. {@link EntityHolder} instances
 * are immutable, thus each change produces a new version of an entity. Also each child update requires update of a parent
 * entity, when the reference to the child changes (a new version is created).
 */
public class EntityHolder {

    private final String id;
    private final Object entity;

    private final SortedSet<EntityHolder> children;
    private final Map<String, Object> attributes;

    private EntityHolder(String id, Object entity, SortedSet<EntityHolder> children, Map<String, Object> attributes) {
        this.id = id;
        this.entity = entity;
        this.children = children;
        this.attributes = attributes;
    }

    public String getId() {
        return id;
    }

    public <E> E getEntity() {
        return (E) entity;
    }

    public SortedSet<EntityHolder> getChildren() {
        return children;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Optional<EntityHolder> findById(String requestedId) {
        if (this.id.equals(requestedId)) {
            return Optional.of(this);
        }
        for (EntityHolder child : children) {
            Optional<EntityHolder> result = child.findById(requestedId);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    public EntityHolder addChild(EntityHolder child) {
        SortedSet<EntityHolder> newChildren = new TreeSet<>(Comparator.comparing(EntityHolder::getId));
        newChildren.addAll(children);

        // Set will not be updated if the child already is there, so explicitly remove it.
        newChildren.remove(child);
        newChildren.add(child);

        return new EntityHolder(id, entity, newChildren, attributes);
    }

    public Pair<EntityHolder, Optional<EntityHolder>> removeChild(String id) {
        return children.stream().filter(c -> c.getId().equals(id)).findFirst().map(removedChild -> {
                    SortedSet<EntityHolder> filteredChildren = new TreeSet<>(Comparator.comparing(EntityHolder::getId));
                    children.forEach(c -> {
                        if (!c.getId().equals(id)) {
                            filteredChildren.add(c);
                        }
                    });
                    EntityHolder newRoot = new EntityHolder(this.id, this.entity, filteredChildren, this.attributes);
                    return Pair.of(newRoot, Optional.of(removedChild));
                }
        ).orElseGet(() -> Pair.of(this, Optional.empty()));
    }

    public EntityHolder addTag(String tagName, Object tagValue) {
        Map<String, Object> newTags = new HashMap<>(attributes);
        newTags.put(tagName, tagValue);
        return new EntityHolder(id, entity, children, newTags);
    }

    public EntityHolder removeTag(String tagName) {
        if (!attributes.containsKey(tagName)) {
            return this;
        }
        Map<String, Object> newTags = new HashMap<>(attributes);
        newTags.remove(tagName);
        return new EntityHolder(id, entity, children, newTags);
    }

    public <E> EntityHolder setEntity(E entity) {
        return new EntityHolder(id, entity, children, attributes);
    }

    public static <E> EntityHolder newRoot(String id, E entity) {
        return new EntityHolder(id, entity, Collections.emptySortedSet(), Collections.emptyMap());
    }
}
