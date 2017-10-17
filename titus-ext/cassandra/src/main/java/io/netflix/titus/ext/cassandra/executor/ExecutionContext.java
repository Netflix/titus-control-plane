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

package io.netflix.titus.ext.cassandra.executor;

import java.util.List;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;

public class ExecutionContext {

    private final Session session;
    private final int pageSize;
    private final int split;

    private final MetadataOperations meta;

    public ExecutionContext(Session session, int pageSize, int split) {
        this.session = session;
        this.pageSize = pageSize;
        this.split = split;
        this.meta = new MetadataOperations(this);
    }

    public Session getSession() {
        return session;
    }

    public int getSplit() {
        return split;
    }

    public List<TokenRange> getTokenRanges() {
        return meta.getRanges();
    }

    public int getPageSize() {
        return pageSize;
    }
}
