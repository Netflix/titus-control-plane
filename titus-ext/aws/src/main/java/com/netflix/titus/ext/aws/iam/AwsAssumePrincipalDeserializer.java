/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.ext.aws.iam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Jackson deserializer for handling AWS assume principals that can be encoded as
 * either an array of principals or a single principal string value.
 */
public class AwsAssumePrincipalDeserializer extends StdDeserializer<AwsAssumePrincipal> {
    private static final Logger logger = LoggerFactory.getLogger(AwsAssumePrincipalDeserializer.class);
    private static final String AWS_KEY = "AWS";

    public AwsAssumePrincipalDeserializer() {
        this(null);
    }

    public AwsAssumePrincipalDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public AwsAssumePrincipal deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException {
        List<String> principals = new ArrayList<>();

        JsonNode node = jp.getCodec().readTree(jp);
        // Find the specific key
        if (node.hasNonNull(AWS_KEY)) {
            JsonNode awsNode = node.get(AWS_KEY);
            // The key's value may be an array or single string element.
            if (awsNode.isArray()) {
                awsNode.forEach(principalNode -> principals.add(principalNode.asText()));
            } else {
                principals.add(awsNode.asText());
            }
        }
        return new AwsAssumePrincipal(principals);
    }
}
