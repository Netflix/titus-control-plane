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

import com.netflix.titus.api.endpoint.v2.rest.representation.EfsMountRepresentation;
import com.netflix.titus.api.endpoint.v2.rest.representation.EfsMountRepresentation.MountPerm;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import com.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EfsValidatorTest {

    private final EfsMountRepresentation EFS_WITH_VALID_MOUNT_POINT = new EfsMountRepresentation("efsId1", "/efsMounts", MountPerm.RW, "/home");

    private final ValidatorConfiguration configuration = mock(ValidatorConfiguration.class);

    private final EfsValidator efsValidator = new EfsValidator(configuration);

    @Before
    public void setUp() throws Exception {
        when(configuration.getInvalidEfsMountPoints()).thenReturn(ValidatorConfiguration.DEFAULT_INVALID_EFS_MOUNT_POINTS);
    }

    @Test
    public void testValidEfsSpec() throws Exception {
        verifyBothVariants(EFS_WITH_VALID_MOUNT_POINT, true);
    }

    @Test
    public void testInvalidEfsMountPointValidation() throws Exception {
        for (String badMountPoint : asList("/data", "/data/", "/data/here")) {
            EfsMountRepresentation badMount = new EfsMountRepresentation("efsId1", badMountPoint, MountPerm.RW, "/home");
            verifyBothVariants(badMount, false);
        }
    }

    private TitusJobSpec newJobSpecWithEfs(EfsMountRepresentation efsMount) {
        return new TitusJobSpec.Builder(
                new TitusV2ModelGenerator(getClass().getSimpleName()).newJobSpec(TitusJobType.batch, "testJob")
        ).efs(efsMount).build();
    }

    private TitusJobSpec newJobSpecWithManyEfs(EfsMountRepresentation... efsMounts) {
        return new TitusJobSpec.Builder(
                new TitusV2ModelGenerator(getClass().getSimpleName()).newJobSpec(TitusJobType.batch, "testJob")
        ).efsMounts(asList(efsMounts)).build();
    }

    private void verifyBothVariants(EfsMountRepresentation mountPoint, boolean expected) {
        assertThat(efsValidator.isValid(newJobSpecWithEfs(mountPoint))).isEqualTo(expected);
        assertThat(efsValidator.isValid(newJobSpecWithManyEfs(mountPoint))).isEqualTo(expected);
    }
}