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

import java.util.Optional;

import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static com.netflix.titus.common.framework.reconciler.EntityHolder.newRoot;
import static com.netflix.titus.common.util.CollectionsExt.first;
import static org.assertj.core.api.Assertions.assertThat;

public class EntityHolderTest {

    @Test
    public void testParentChildRelationShip() throws Exception {
        EntityHolder child = newRoot("myChild", "a1");
        EntityHolder root = newRoot("myRoot", "as").addChild(child);

        // Check root
        assertThat(root.getId()).isEqualTo("myRoot");
        assertThat((String) root.getEntity()).isEqualTo("as");
        assertThat(root.getChildren()).hasSize(1);
        assertThat(first(root.getChildren()) == child).isTrue();

        // Check child
        assertThat(child.getId()).isEqualTo("myChild");
        assertThat((String) child.getEntity()).isEqualTo("a1");
    }

    @Test
    public void testExistingChildGetsUpdated() throws Exception {
        EntityHolder rootV1 = newRoot("myRoot", "as").addChild(newRoot("myChild", "a1"));
        EntityHolder rootV2 = rootV1.addChild(newRoot("myChild", "a1_v2"));

        assertThat((String) first(rootV2.getChildren()).getEntity()).isEqualTo("a1_v2");
    }

    @Test
    public void testRemoveFromParent() throws Exception {
        EntityHolder rootV1 = newRoot("myRoot", "as")
                .addChild(newRoot("myChild1", "a1"))
                .addChild(newRoot("myChild2", "a2"));

        Pair<EntityHolder, Optional<EntityHolder>> rootChildPair = rootV1.removeChild("myChild1");
        EntityHolder rootV2 = rootChildPair.getLeft();
        EntityHolder child1 = rootChildPair.getRight().get();

        assertThat(rootV2.getChildren()).hasSize(1);
        assertThat(first(rootV2.getChildren()).getId()).isEqualTo("myChild2");
        assertThat(child1.getId()).isEqualTo("myChild1");
    }
}