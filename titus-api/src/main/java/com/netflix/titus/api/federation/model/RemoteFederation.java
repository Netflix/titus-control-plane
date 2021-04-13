package com.netflix.titus.api.federation.model;

public final class RemoteFederation {
    private final String address;

    public RemoteFederation(String address) {
        this.address = address;
    }

    public String getAddress() {
        return address;
    }
}
