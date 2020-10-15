package com.netflix.titus.ext.aws.appscale;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.runtime.endpoint.metadata.spring.CallMetadataAuthentication;
import org.junit.Test;
import org.springframework.http.ResponseEntity;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AppAutoScalingCallbackSpringResourceTest {

    @Test
    public void testResponseEntity() {
        AppAutoScalingCallbackService appAutoScalingCallbackService = mock(AppAutoScalingCallbackService.class);
        String jobId = "job-1";
        ScalableTargetResourceInfo scalableTargetResourceInfo = ScalableTargetResourceInfo.newBuilder().actualCapacity(4).desiredCapacity(6).scalingStatus(
                DefaultAppAutoScalingCallbackService.ScalingStatus.Pending.name()).build();
        CallMetadata callMetadata = CallMetadata.newBuilder().withCallerId("unit-testing").build();
        CallMetadataAuthentication callMetadataAuthentication = mock(CallMetadataAuthentication.class);
        when(callMetadataAuthentication.getCallMetadata()).thenReturn(callMetadata);

        when(appAutoScalingCallbackService.setScalableTargetResourceInfo(jobId, scalableTargetResourceInfo, callMetadata))
                .thenReturn(Observable.just(scalableTargetResourceInfo));
        AppAutoScalingCallbackSpringResource springResource = new AppAutoScalingCallbackSpringResource(appAutoScalingCallbackService);
        ResponseEntity<ScalableTargetResourceInfo> resp = springResource.setScalableTargetResourceInfo(jobId,
                scalableTargetResourceInfo, callMetadataAuthentication);
        assertThat(resp).isNotNull();
        assertThat(resp.getStatusCodeValue()).isEqualTo(200);
        assertThat(resp.getBody()).isEqualTo(scalableTargetResourceInfo);

        scalableTargetResourceInfo.setDesiredCapacity(-1);
        ResponseEntity<ScalableTargetResourceInfo> resp2 = springResource.setScalableTargetResourceInfo(jobId,
                scalableTargetResourceInfo, callMetadataAuthentication);
        assertThat(resp2).isNotNull();
        assertThat(resp2.getStatusCodeValue()).isEqualTo(400);
    }

}