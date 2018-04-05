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

package com.netflix.titus.runtime.endpoint.v3.grpc;


import com.netflix.titus.api.scheduler.model.Match;
import com.netflix.titus.api.scheduler.model.Must;
import com.netflix.titus.api.scheduler.model.Operator;
import com.netflix.titus.api.scheduler.model.Should;
import com.netflix.titus.api.scheduler.model.SystemSelector;

public class GrpcSchedulerModelConverters {
    // GRPC Converters
    public static com.netflix.titus.grpc.protogen.SystemSelector toGrpcSystemSelector(SystemSelector systemSelector) {
        com.netflix.titus.grpc.protogen.SystemSelector.Builder builder = com.netflix.titus.grpc.protogen.SystemSelector.newBuilder()
                .setId(systemSelector.getId())
                .setDescription(systemSelector.getDescription())
                .setEnabled(systemSelector.isEnabled())
                .setPriority(systemSelector.getPriority())
                .setReason(systemSelector.getReason())
                .setTimestamp(systemSelector.getTimestamp());

        if (systemSelector.getShould() != null) {
            builder.setShould(toGrpcShould(systemSelector.getShould()));
        } else if (systemSelector.getMust() != null) {
            builder.setMust(toGrpcMust(systemSelector.getMust()));
        }
        return builder.build();
    }

    public static com.netflix.titus.grpc.protogen.Should toGrpcShould(Should should) {
        Operator operator = should.getOperator();
        com.netflix.titus.grpc.protogen.Should.Builder builder = com.netflix.titus.grpc.protogen.Should.newBuilder();
        if (operator instanceof Match) {
            builder.setMatch(toGrpcMatch((Match) operator));
        } else {
            throw new IllegalStateException("Unknown operator: " + operator);
        }
        return builder.build();
    }

    public static com.netflix.titus.grpc.protogen.Must toGrpcMust(Must must) {
        Operator operator = must.getOperator();
        com.netflix.titus.grpc.protogen.Must.Builder builder = com.netflix.titus.grpc.protogen.Must.newBuilder();
        if (operator instanceof Match) {
            builder.setMatch(toGrpcMatch((Match) operator));
        } else {
            throw new IllegalStateException("Unknown operator: " + operator);
        }
        return builder.build();
    }

    public static com.netflix.titus.grpc.protogen.Match toGrpcMatch(Match match) {
        return com.netflix.titus.grpc.protogen.Match.newBuilder()
                .setSelectExpression(match.getSelectExpression())
                .setMatchExpression(match.getMatchExpression())
                .build();
    }

    //Core Converters
    public static SystemSelector toCoreSystemSelector(com.netflix.titus.grpc.protogen.SystemSelector systemSelector) {
        SystemSelector.Builder builder = SystemSelector.newBuilder()
                .withId(systemSelector.getId())
                .withDescription(systemSelector.getDescription())
                .withEnabled(systemSelector.getEnabled())
                .withPriority(systemSelector.getPriority())
                .withReason(systemSelector.getReason())
                .withTimestamp(systemSelector.getTimestamp());

        switch (systemSelector.getTypeCase()) {
            case SHOULD:
                builder.withShould(toCoreShould(systemSelector.getShould()));
                break;
            case MUST:
                builder.withMust(toCoreMust(systemSelector.getMust()));
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + systemSelector.getTypeCase());
        }
        return builder.build();
    }

    public static Should toCoreShould(com.netflix.titus.grpc.protogen.Should should) {
        switch (should.getOperatorCase()) {
            case MATCH:
                return Should.newBuilder().withOperator(toCoreMatch(should.getMatch())).build();
            default:
                throw new IllegalArgumentException("Unknown operator: " + should.getOperatorCase());
        }
    }

    public static Must toCoreMust(com.netflix.titus.grpc.protogen.Must must) {
        switch (must.getOperatorCase()) {
            case MATCH:
                return Must.newBuilder().withOperator(toCoreMatch(must.getMatch())).build();
            default:
                throw new IllegalArgumentException("Unknown operator: " + must.getOperatorCase());
        }
    }

    public static Match toCoreMatch(com.netflix.titus.grpc.protogen.Match match) {
        return Match.newBuilder()
                .withSelectExpression(match.getSelectExpression())
                .withMatchExpression(match.getMatchExpression())
                .build();
    }
}
