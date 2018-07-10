package com.netflix.titus.api.jobmanager.model.job.sanitizer;

import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.model.ResourceDimension;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobAssertionsTest {

    @Test
    public void testImageDigestValidation() {
        Image image = Image.newBuilder()
                .withName("imageName")
                .withDigest("sha256:abcdef0123456789abcdef0123456789abcdef0123456789")
                .build();
        Map<String, String> violations = new JobAssertions(id -> ResourceDimension.empty()).validateImage(image);
        assertThat(violations).isEmpty();
    }

    @Test
    public void testInvalidDigestValidation() {
        Image image = Image.newBuilder()
                .withName("imageName")
                .withDigest("sha256:XYZ")
                .build();
        Map<String, String> violations = new JobAssertions(id -> ResourceDimension.empty()).validateImage(image);
        assertThat(violations).hasSize(1);
    }
}