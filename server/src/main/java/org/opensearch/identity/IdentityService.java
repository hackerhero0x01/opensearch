/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.common.settings.Settings;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.scopes.ScopeEnums;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.identity.noop.NoopIdentityPlugin;
import org.opensearch.plugins.IdentityPlugin;

/**
 * Identity and access control for OpenSearch.
 *
 * @opensearch.experimental
 * */
public class IdentityService {
    private static final Logger log = LogManager.getLogger(IdentityService.class);

    private final Settings settings;
    private final IdentityPlugin identityPlugin;
    private List<Principal> applicationList = List.of(); // A list of known application principals

    private static IdentityService instance = null;

    public IdentityService(final Settings settings, final List<IdentityPlugin> identityPlugins) {
        this.settings = settings;

        if (identityPlugins.size() == 0) {
            log.debug("Identity plugins size is 0");
            identityPlugin = new NoopIdentityPlugin();
        } else if (identityPlugins.size() == 1) {
            log.debug("Identity plugins size is 1");
            identityPlugin = identityPlugins.get(0);
        } else {
            throw new OpenSearchException(
                "Multiple identity plugins are not supported, found: "
                    + identityPlugins.stream().map(Object::getClass).map(Class::getName).collect(Collectors.joining(","))
            );
        }
        IdentityService.instance = this;
    }

    /**
     * Gets the current subject
     */
    public ApplicationAwareSubject getSubject() {
        return new ApplicationAwareSubject(identityPlugin.getSubject());
    }

    /**
     * Gets the token manager
     */
    public TokenManager getTokenManager() {
        return identityPlugin.getTokenManager();
    }

    public static IdentityService getInstance() {
        if (instance == null) {
            new IdentityService(Settings.EMPTY, List.of());
        }
        return instance;
    }

    /**
     * Sets the application list known to the IdentityService
     * @param applicationList A list of all principals for known applications
     */
    public void setApplicationList(List<Principal> applicationList) {
        this.applicationList = List.copyOf(applicationList);
    }

    /**
     * Returns a list of the known application principals
     */
    public List<Principal> getApplicationList() {
        return List.copyOf(applicationList);
    }

    /**
     * Returns a list of the known application principals
     */
    public List<String> getApplicationStrings() {
        return applicationList.stream().map(Principal::getName).collect(Collectors.toList());
    }


    //TODO: Find a way to combine these
    public Set<String> getApplicationScopes(Principal principal) {

        Set<String> allScopes = ExtensionsManager.getExtensionManager().getExtensionIdMap().get(principal.getName()).getScopes();

        return allScopes.stream().filter(scope -> {
            String[] parts = scope.split("\\.");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid scope format: " + scope);
            }
            ScopeEnums.ScopeNamespace scopeNamespace = ScopeEnums.ScopeNamespace.fromString(parts[0]);
            return scopeNamespace == ScopeEnums.ScopeNamespace.APPLICATION;
        }).collect(Collectors.toSet());
    }

    public Set<String> getActionScopes(Principal principal) {

        Set<String> allScopes = ExtensionsManager.getExtensionManager().getExtensionIdMap().get(principal.getName()).getScopes();

        return allScopes.stream().filter(scope -> {
            String[] parts = scope.split("\\.");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid scope format: " + scope);
            }
            ScopeEnums.ScopeNamespace scopeNamespace = ScopeEnums.ScopeNamespace.fromString(parts[0]);
            return scopeNamespace == ScopeEnums.ScopeNamespace.ACTION;
        }).collect(Collectors.toSet());
    }

    public Set<String> getExtensionPointScopes(Principal principal) {

        Set<String> allScopes = ExtensionsManager.getExtensionManager().getExtensionIdMap().get(principal.getName()).getScopes();

        return allScopes.stream().filter(scope -> {
            String[] parts = scope.split("\\.");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid scope format: " + scope);
            }
            ScopeEnums.ScopeNamespace scopeNamespace = ScopeEnums.ScopeNamespace.fromString(parts[0]);
            return scopeNamespace == ScopeEnums.ScopeNamespace.EXTENSION_POINT;
        }).collect(Collectors.toSet());
    }
}
