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

package com.netflix.titus.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.titus.common.util.tuple.Pair;

/**
 * A collection of methods for property name/values processing.
 */
public final class PropertiesExt {

    private PropertiesExt() {
    }

    /**
     * Load properties from classpath.
     */
    public static Optional<Map<String, String>> loadFromClassPath(String resourceName) throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        if (is == null) {
            return Optional.empty();
        }
        Properties properties = new Properties();
        properties.load(is);
        Map<String, String> mapInstance = properties.entrySet()
                .stream()
                .collect(Collectors.toMap(p -> (String) p.getKey(), p -> (String) p.getValue()));
        return Optional.of(mapInstance);
    }

    /**
     * Group properties by their name's root part. For example properties ['my.a', 'my.b', 'your.x', 'your.y'], would
     * be in groups 'my' and 'your', with property names ['a', 'b'] and ['x', 'y'] respectively.
     */
    public static Map<String, Map<String, String>> groupByRootName(Map<String, String> properties, int rootParts) {
        if (CollectionsExt.isNullOrEmpty(properties)) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, String>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Pair<String, String> parts = split(entry.getKey(), rootParts);
            if (parts == null) {
                continue;
            }
            String rootPart = parts.getLeft();
            String internalName = parts.getRight();

            Map<String, String> internalProps = result.get(rootPart);
            if (internalProps == null) {
                result.put(rootPart, internalProps = new HashMap<>());
            }
            internalProps.put(internalName, entry.getValue());
        }
        return result;
    }

    /**
     * Returns properties with the common name prefix.
     */
    public static Map<String, String> getPropertiesOf(String namePrefix, Map<String, String> properties) {
        if (CollectionsExt.isNullOrEmpty(properties)) {
            return Collections.emptyMap();
        }
        String namePrefixWithDot = namePrefix + '.';
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String name = entry.getKey().trim();
            if (name.isEmpty() || !name.startsWith(namePrefixWithDot)) {
                continue;
            }
            String internalName = name.substring(namePrefixWithDot.length());
            result.put(internalName, entry.getValue());
        }
        return result;
    }

    /**
     * For given properties, examine their names, and check that each property name's root part is in the expected
     * name set. For example property names ['my.a', 'your'], have their root parts equal to ['my', 'your']. The
     * latter set must be a subset of the expected root names.
     *
     * @return a collection of not matching property names (or empty list if all match)
     */
    public static List<String> verifyRootNames(Map<String, String> properties, Set<String> expectedRootNames) {
        if (CollectionsExt.isNullOrEmpty(properties)) {
            return Collections.emptyList();
        }
        List<String> notMatching = new ArrayList<>();
        for (String key : properties.keySet()) {
            String name = key.trim();

            int idx = name.indexOf('.');
            String rootPart = (idx == -1 || idx == name.length() - 1)
                    ? name
                    : name.substring(0, idx);

            if (!expectedRootNames.contains(rootPart)) {
                notMatching.add(name);
            }
        }
        return notMatching;
    }

    /**
     * Split property names into two parts, where the first part may include one or more segments separated with dot.
     * The second part may be null, if the name is too short.
     */
    public static Map<String, Set<String>> splitNames(Collection<String> names, int rootSegments) {
        // Split nested field names
        Map<String, Set<String>> topNames = new HashMap<>();
        names.forEach(n -> {
            Pair<String, String> parts = split(n, rootSegments);
            if (parts == null) {
                topNames.put(n, null);
            } else {
                Set<String> nested = topNames.get(parts.getLeft());
                if (nested == null) {
                    nested = new HashSet<>();
                    topNames.put(parts.getLeft(), nested);
                }
                nested.add(parts.getRight());
            }
        });
        return topNames;
    }

    /**
     * Split property name into two parts, where the first part may include one or more segments separated with dot.
     * The second part may be null, if the name is too short.
     */
    public static Pair<String, String> split(String name, int rootParts) {
        if (name.isEmpty()) {
            return null;
        }
        int idx = 0;
        for (int i = 0; i < rootParts && (idx = name.indexOf('.', idx)) != -1; i++) {
            idx++;
        }
        if (idx == -1 || idx == name.length()) {
            return null;
        }
        return Pair.of(name.substring(0, idx - 1), name.substring(idx));
    }

    /**
     * Split property names into dot separated parts in a tree form.
     */
    public static PropertyNode<Boolean> fullSplit(Collection<String> names) {
        PropertyNode<Boolean> root = new PropertyNode<>("ROOT", Optional.empty(), new HashMap<>());
        names.forEach(n -> {
            List<String> parts = StringExt.splitByDot(n);
            PropertyNode<Boolean> current = root;
            int lastIdx = parts.size() - 1;
            for (int i = 0; i < parts.size(); i++) {
                String name = parts.get(i);

                PropertyNode<Boolean> next = current.getChildren().get(name);
                if (next == null) {
                    next = new PropertyNode<>(name, Optional.empty(), new HashMap<>());
                    current.getChildren().put(name, next);
                }

                if (i == lastIdx) {
                    next.value = Optional.of(Boolean.TRUE);
                }
                current = next;
            }
        });
        return root;
    }

    public static class PropertyNode<V> {
        private final String name;
        private Optional<V> value;
        private final Map<String, PropertyNode<V>> children;

        private PropertyNode(String name, Optional<V> value, Map<String, PropertyNode<V>> children) {
            this.name = name;
            this.value = value;
            this.children = children;
        }

        public String getName() {
            return name;
        }

        public Optional<V> getValue() {
            return value;
        }

        public Map<String, PropertyNode<V>> getChildren() {
            return children;
        }
    }
}
