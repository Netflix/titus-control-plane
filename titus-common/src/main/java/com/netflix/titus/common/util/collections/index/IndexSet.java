/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.util.collections.index;

import java.util.Collection;
import java.util.Map;

public interface IndexSet<PRIMARY_KEY, INPUT> {

    Map<String, Group<?, PRIMARY_KEY, ?>> getGroups();

    <GROUP_KEY, OUTPUT> Group<GROUP_KEY, PRIMARY_KEY, OUTPUT> getGroup(String groupId);

    Map<String, Index<?, ?>> getIndexes();

    <UNIQUE_INDEX_KEY, OUTPUT> Index<UNIQUE_INDEX_KEY, OUTPUT> getIndex(String indexId);

    Map<String, Order<?>> getOrders();

    <OUTPUT> Order<OUTPUT> getOrder(String orderId);

    IndexSet<PRIMARY_KEY, INPUT> add(Collection<INPUT> values);

    IndexSet<PRIMARY_KEY, INPUT> remove(Collection<PRIMARY_KEY> values);
}
