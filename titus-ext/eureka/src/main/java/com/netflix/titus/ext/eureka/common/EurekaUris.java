/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.ext.eureka.common;

import java.net.URI;
import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import org.springframework.web.util.UriComponentsBuilder;

public class EurekaUris {

    public static final String EUREKA_SCHEME = "eureka";
    public static final String EUREKA_SECURE_PARAM = "secure";

    public static URI failIfEurekaUriInvalid(URI eurekaUri) {
        Preconditions.checkArgument(isEurekaScheme(eurekaUri), "Not Eureka scheme in URI: %s", eurekaUri);
        Preconditions.checkArgument(StringExt.isNotEmpty(eurekaUri.getHost()), "Service name not defined in URI: %s", eurekaUri);
        return eurekaUri;
    }

    public static String getServiceName(URI eurekaUri) {
        return eurekaUri.getHost();
    }

    public static boolean isEurekaScheme(URI eurekaUri) {
        return EUREKA_SCHEME.equalsIgnoreCase(eurekaUri.getScheme());
    }

    public static boolean isSecure(URI eurekaUri) {
        List<String> secureValue = UriComponentsBuilder.fromUri(eurekaUri).build().getQueryParams().get(EUREKA_SECURE_PARAM);
        if (CollectionsExt.isNullOrEmpty(secureValue)) {
            return false;
        }
        String value = StringExt.safeTrim(secureValue.get(0));
        return value.isEmpty() || "true".equalsIgnoreCase(value);
    }
}
