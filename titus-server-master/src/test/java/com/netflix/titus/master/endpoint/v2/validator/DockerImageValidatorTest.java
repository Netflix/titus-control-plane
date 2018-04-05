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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DockerImageValidatorTest {

    @Test
    public void validateAllRegistryImageNames() throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream("reg-list");
        final String resp = new BufferedReader(new InputStreamReader(resourceAsStream)).lines().collect(Collectors.joining("\n"));
        final CollectionType respType = objectMapper.getTypeFactory().constructCollectionType(List.class, String.class);
        final List<String> imageList = objectMapper.readValue(resp, respType);

        final DockerImageValidator dockerImageValidator = new DockerImageValidator();

        imageList.stream().forEach(image -> {
            // keep version null
            final TitusJobSpec tj = new TitusJobSpec.Builder().applicationName(image).build();
            assertTrue(image + " should be valid", dockerImageValidator.isValid(tj));
        });
        System.out.println("Validation done on " + imageList.size() + " images.");
    }

    @Test
    public void validateAllRegistryImageTags() throws IOException {
        final InputStream tagsResourceAsStream = this.getClass().getClassLoader().getResourceAsStream("tags-list");
        final Stream<String> tagsList = new BufferedReader(new InputStreamReader(tagsResourceAsStream)).lines();
        final DockerImageValidator dockerImageValidator = new DockerImageValidator();

        final long tagsCount = tagsList.map(imageTag -> {
            final TitusJobSpec tj = new TitusJobSpec.Builder().version(imageTag).applicationName("trustybase").build();
            assertTrue(imageTag + " should be valid", dockerImageValidator.isValid(tj));
            return true;
        }).count();
        System.out.println("Validation done on " + tagsCount + " tags.");
    }

    @Test
    public void checkImageNames() {
        assertTrue("Image can have \\ or /", DockerImageValidator.verifyImageNameFormat("foo\\bar/foobar"));
        assertFalse("Image name can not have spaces", DockerImageValidator.verifyImageNameFormat("I am invalid."));
        assertFalse("Image name can not have colon", DockerImageValidator.verifyImageNameFormat("ubuntu:latest"));
        assertFalse("Image name can not have first char as .", DockerImageValidator.verifyImageNameFormat(".ubuntu"));
        assertFalse("Image name can not have last char as .", DockerImageValidator.verifyImageNameFormat("ubuntu."));
        assertFalse("Image name can not have first char as _", DockerImageValidator.verifyImageNameFormat("_ubuntu"));
        assertFalse("Image name can not have last char as _", DockerImageValidator.verifyImageNameFormat("ubuntu_"));
        assertFalse("Image name can not have first char as -", DockerImageValidator.verifyImageNameFormat("-ubuntu"));
        assertFalse("Image name can not have last char as -", DockerImageValidator.verifyImageNameFormat("ubuntu-"));
    }

    @Test
    public void checkTagNames() {
        assertTrue("Tag name can be null", DockerImageValidator.verifyTagFormat(null));
        assertFalse("Tag name can not be empty", DockerImageValidator.verifyTagFormat("   "));
        assertFalse("Tag name can not have \\ or /", DockerImageValidator.verifyTagFormat("foo\\bar"));
        assertFalse("Tag name can not have spaces", DockerImageValidator.verifyTagFormat("I am invalid."));
        assertFalse("Tag name can not have semi colon", DockerImageValidator.verifyTagFormat("v2345;90"));
        assertFalse("Tag name can not have colon", DockerImageValidator.verifyTagFormat("v2345:90"));
        assertFalse("Tag name can not have first char as .", DockerImageValidator.verifyTagFormat(".v234590"));
        assertFalse("Tag name can not have first char as -", DockerImageValidator.verifyTagFormat("-v234590"));
    }


}
