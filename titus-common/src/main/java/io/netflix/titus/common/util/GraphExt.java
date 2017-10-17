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

package io.netflix.titus.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Graph algorithm implementations.
 */
public class GraphExt {

    /**
     * In the directed graph, given a list of nodes and their edges, return an ordered list of nodes. The ordering
     * rule is that if there is a path from node A to B, the node A is ahead of the node B in the list. This is a
     * stable sort, so services order is intact unless necessary.
     * <p>
     * FIXME this is a quick and dirty implementation that works for the single edge scenario only (which we need in ActivationProvisionListener)
     */
    public static <T> List<T> order(List<T> nodes, BiFunction<T, T, Boolean> hasEdge) {
        List<T> result = new ArrayList<>(nodes);

        for (int i = 0; i < nodes.size(); i++) {
            for (int j = 0; j < nodes.size(); j++) {
                if (i != j) {
                    T from = nodes.get(i);
                    T to = nodes.get(j);
                    // There is an edge from right to left, so we need to move left element after right
                    if (hasEdge.apply(from, to) && i > j) {
                        result.remove(j);
                        result.add(i, to);
                        return result;
                    }
                }
            }
        }
        return result;
    }
}
