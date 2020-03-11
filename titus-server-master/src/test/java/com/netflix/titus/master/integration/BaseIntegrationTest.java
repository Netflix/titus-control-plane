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

package com.netflix.titus.master.integration;

import java.security.Permission;

import com.netflix.titus.testkit.junit.category.IntegrationTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class BaseIntegrationTest {

    protected static final long SHORT_TIMEOUT_MS = 5_000;

    protected static final long TEST_TIMEOUT_MS = 30_000;

    protected static final long LONG_TEST_TIMEOUT_MS = 60_000;

    static class PreventSystemExitSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(Permission perm) {
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
        }

        @Override
        public void checkExit(int status) {
            if (status != 0) {
                String message = "System exit requested with error " + status;
                throw new IllegalStateException(message);
            }
        }
    }

    private static final SecurityManager securityManager = new PreventSystemExitSecurityManager();

    @BeforeClass
    public static void setSecurityManager() {
        if (System.getSecurityManager() != securityManager) {
            System.setSecurityManager(securityManager);
        }
    }
}
