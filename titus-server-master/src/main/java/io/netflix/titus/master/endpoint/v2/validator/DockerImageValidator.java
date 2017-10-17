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

package io.netflix.titus.master.endpoint.v2.validator;

import java.util.regex.Pattern;

import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

public class DockerImageValidator implements TitusJobSpecValidators.Validator {
    private static Pattern imagePattern = Pattern.compile("[a-zA-Z0-9\\.\\\\/_-]+");
    private static Pattern tagPattern = Pattern.compile("[a-zA-Z0-9\\._-]+");


    public boolean isValid(TitusJobSpec titusJobSpec) {
        return verifyImageNameFormat(titusJobSpec.getApplicationName()) && verifyTagFormat(titusJobSpec.getVersion());
    }

    public static boolean verifyImageNameFormat(String name) {
        if (name == null || name.trim().isEmpty()) {
            return false;
        }

        final char firstChar = name.charAt(0);
        final char lastChar = name.charAt(name.length() - 1);
        return imagePattern.matcher(name).matches() &&
                !isImageNameSpecialChar(firstChar) &&
                !isImageNameSpecialChar(lastChar);
    }


    public static boolean isImageNameSpecialChar(char c) {
        return c == '.' || c == '_' || c == '-';
    }

    public static boolean isTagSpecialChar(char c) {
        return c == '.' || c == '-';
    }

    public static boolean verifyTagFormat(String tag) {
        if (tag == null) {
            return true;
        }
        if (tag.isEmpty()) {
            return false;
        }

        final char firstChar = tag.charAt(0);
        return tagPattern.matcher(tag).matches() &&
                !isTagSpecialChar(firstChar) &&
                tag.length() <= 127;
    }
}
