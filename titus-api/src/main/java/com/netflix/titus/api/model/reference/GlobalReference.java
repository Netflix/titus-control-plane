package com.netflix.titus.api.model.reference;

import com.netflix.titus.api.model.Level;

public class GlobalReference extends Reference {
    private static final com.netflix.titus.api.model.reference.GlobalReference INSTANCE = new com.netflix.titus.api.model.reference.GlobalReference();

    private GlobalReference() {
        super(Level.Global);
    }

    @Override
    public String getName() {
        return "global";
    }

    public static GlobalReference getInstance() {
        return INSTANCE;
    }
}
