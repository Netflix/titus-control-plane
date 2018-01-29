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

package io.netflix.titus.common.util.archaius2;

import java.util.Properties;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.archaius.api.Config;
import com.netflix.archaius.api.config.CompositeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write the resolved archaius2 configuration to the log file. This is one time operation executed
 * during the container bootstrap.
 */
@Singleton
public class Archaius2ConfigurationLogger {

    private static final Logger logger = LoggerFactory.getLogger(Archaius2ConfigurationLogger.class);

    @Inject
    public Archaius2ConfigurationLogger(Config config) {
        Properties props = new Properties();
        config.getKeys().forEachRemaining(k -> props.put(k, config.getString(k)));

        // Print loaded properties
        config.accept(new ConfigLogVisitor());
    }

    private static class ConfigLogVisitor implements CompositeConfig.CompositeVisitor<Void> {

        private String prefix = "";

        @Override
        public Void visitKey(String key, Object value) {
            logger.info(prefix + key + " = " + value);
            return null;
        }

        @Override
        public Void visitChild(String name, Config child) {
            logger.info(prefix + "Config: " + name);
            prefix += "  ";
            child.accept(this);
            prefix = prefix.substring(0, prefix.length() - 2);
            return null;
        }
    }
}
