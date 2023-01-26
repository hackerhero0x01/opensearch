/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that defines a Permission.
 *
 * Example "cluster.write"
 *
 * @opensearch.experimental
 */
public class OpenSearchPermission implements Permission {

    public final String PERMISSION_DELIMITER = "\\.";

    public final String ACTION_DELIMITER = "/";

    public String permissionString;

    public String[] permissionSegments;

    public String resource;

    public String action;
    public String permissionType;

    public OpenSearchPermission(String permission) {

        this.permissionString = permission;
        try {
            this.permissionSegments = permissionString.split(PERMISSION_DELIMITER);
            this.permissionType = permissionSegments[0];
            this.action = permissionSegments[1];
        } catch (IndexOutOfBoundsException ex) {
            throw new PermissionFactory.InvalidPermissionException(
                "All permissions must contain a permission type and" + " action delimited by a \".\"."
            );
        }
        if (this.permissionSegments.length == 3) {
            this.resource = permissionSegments[2];
        }
    }

    /**
     * Compare the current permission's permission type to another permission's permission type
     */
    public boolean permissionTypesMatch(OpenSearchPermission permission) {

        if (this.permissionType.equalsIgnoreCase(permission.permissionType)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean implies(Permission permission) {

        OpenSearchPermission permissionToCompare = new OpenSearchPermission(permission.toString());

        // Check if permission types match
        if (!permissionTypesMatch(permissionToCompare)) {
            return false;
        }

        // Check if action namespaces match or this permission's namespace includes the targeted permission
        WildcardPermission wildcardPermission = new WildcardPermission(this.action.replace("/", ":"));
        if (!wildcardPermission.implies(new WildcardPermission(permissionToCompare.action.replace("/", ":'")))) {
            return false;
        }

        // Check that resource patterns match or that this permission's resource pattern includes the targeted permission
        if (this.resource != null) {
            String[] resourcePatterns = this.resource.split(",");
            for (String resource : resourcePatterns) {
                Pattern pattern = Pattern.compile(resource, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(permissionToCompare.resource);
                if (matcher.find()) {
                    return true;
                }
            }
        } else if (this.resource == null && permissionToCompare.resource == null) {
            return true;
        }
        return false;
    }
}
