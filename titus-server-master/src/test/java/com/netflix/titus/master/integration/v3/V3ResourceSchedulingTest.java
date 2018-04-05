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

package com.netflix.titus.master.integration.v3;

import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 */
@Category(IntegrationTest.class)
public class V3ResourceSchedulingTest extends BaseIntegrationTest {
    /**
     * TODO Implement once V3 service job is supported.
     * <p>
     * Verify ENI assignment
     */
    @Test(timeout = 30_000)
    public void checkIpPerEniLimitIsPreserved() throws Exception {
    }
}
