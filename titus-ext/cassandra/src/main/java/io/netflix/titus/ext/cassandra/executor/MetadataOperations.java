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

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetadataOperations {

    private static final Logger logger = LoggerFactory.getLogger(MetadataOperations.class);

    private Session session;
    private int split;
    private final List<TokenRange> ranges;

    public MetadataOperations(ExecutionContext context) {
        this.session = context.getSession();
        this.split = context.getSplit();
        this.ranges = buildTokenRanges();
    }

    public List<TokenRange> getRanges() {
        return ranges;
    }

    private List<TokenRange> buildTokenRanges() {
        List<TokenRange> result = new ArrayList<>();
        Metadata metadata = session.getCluster().getMetadata();
        for (TokenRange range : metadata.getTokenRanges()) {
            for (TokenRange split : range.splitEvenly(split)) {
                result.addAll(split.unwrap());
            }
        }
        logger.info("Configured with {} token ranges, and {} splits", metadata.getTokenRanges(), result.size());
        return result;
    }
}
