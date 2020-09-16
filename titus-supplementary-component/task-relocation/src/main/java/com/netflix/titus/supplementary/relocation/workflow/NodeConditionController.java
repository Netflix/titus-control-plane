package com.netflix.titus.supplementary.relocation.workflow;

import com.netflix.titus.api.common.LeaderActivationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public interface NodeConditionController extends LeaderActivationListener {
}
