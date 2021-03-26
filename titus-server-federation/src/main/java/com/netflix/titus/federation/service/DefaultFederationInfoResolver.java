package com.netflix.titus.federation.service;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.federation.model.Federation;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class DefaultFederationInfoResolver implements FederationInfoResolver {
    private static final Logger logger = LoggerFactory.getLogger(DefaultFederationInfoResolver.class);
    private final TitusFederationConfiguration configuration;

    @Inject
    public DefaultFederationInfoResolver(TitusFederationConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * @throws IllegalStateException when no federation endpoint is configured
     */
    @Override
    public Federation resolve() {
        String configuredFederation = configuration.getFederationEndpoint();
        List<Cell> cells = CellInfoUtil.extractCellsFromCellSpecification(configuredFederation);
        if (cells.size() != 1) {
            throw new IllegalStateException("No valid federation was configured: " + configuredFederation);
        }

        Cell cell = cells.get(0);
        return new Federation(cell.getName(), cell.getAddress());
    }
}
