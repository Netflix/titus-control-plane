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

package io.netflix.titus.testkit.embedded.cloud.agent;

import io.netflix.titus.testkit.util.PrettyPrinters;
import org.apache.mesos.Protos;

public class OfferChangeEvent {

    private final Protos.Offer offer;
    private final boolean rescind;

    public OfferChangeEvent(Protos.Offer offer, boolean rescind) {
        this.offer = offer;
        this.rescind = rescind;
    }

    public Protos.Offer getOffer() {
        return offer;
    }

    public boolean isRescind() {
        return rescind;
    }

    @Override
    public String toString() {
        return "OfferChangeEvent{" +
                "offer=" + PrettyPrinters.printCompact(offer) +
                ", rescind=" + rescind +
                '}';
    }

    public static OfferChangeEvent rescind(Protos.Offer offer) {
        return new OfferChangeEvent(offer, true);
    }

    public static OfferChangeEvent offer(Protos.Offer offer) {
        return new OfferChangeEvent(offer, false);
    }
}
