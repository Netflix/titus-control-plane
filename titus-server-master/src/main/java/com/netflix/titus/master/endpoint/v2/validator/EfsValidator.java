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

package com.netflix.titus.master.endpoint.v2.validator;

import java.util.List;

import com.netflix.titus.api.endpoint.v2.rest.representation.EfsMountRepresentation;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

import static com.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;

public class EfsValidator implements TitusJobSpecValidators.Validator {

    private final ValidatorConfiguration configuration;

    public EfsValidator(ValidatorConfiguration configuration) {
        this.configuration = configuration;
    }

    public boolean isValid(TitusJobSpec titusJobSpec) {
        if (titusJobSpec.getEfs() != null && !isValid(titusJobSpec.getEfs())) {
            return false;
        }
        if (!isNullOrEmpty(titusJobSpec.getEfsMounts())) {
            for (EfsMountRepresentation efsMount : titusJobSpec.getEfsMounts()) {
                if (!isValid(efsMount)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isValid(EfsMountRepresentation efsMount) {
        // Need to specify both a EFS ID and a mountpoint
        if (efsMount.getEfsId() == null && efsMount.getMountPoint() == null) {
            return true;
        }
        if (efsMount.getEfsId() == null || efsMount.getMountPoint() == null) {
            return false;
        }
        return isOutsideOfInvalidMountPoints(efsMount.getMountPoint(), configuration.getInvalidEfsMountPoints());
    }

    private boolean isOutsideOfInvalidMountPoints(String mountPoint, List<String> restrictedPaths) {
        for (String restrictedPath : restrictedPaths) {
            if (isOverlappingWithRestrictedPath(mountPoint, restrictedPath)) {
                return false;
            }
        }
        return true;
    }

    private boolean isOverlappingWithRestrictedPath(String mountPoint, String restrictedPath) {
        String restrictedPathWithSlash = restrictedPath.endsWith("/") ? restrictedPath : restrictedPath + '/';
        return mountPoint.equals(restrictedPath) || mountPoint.startsWith(restrictedPathWithSlash);
    }
}
