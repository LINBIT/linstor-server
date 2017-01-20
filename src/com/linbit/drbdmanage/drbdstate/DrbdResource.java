package com.linbit.drbdmanage.drbdstate;

import java.util.Map;
import java.util.TreeMap;

/**
 * Tracks the state of a kernel DRBD resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DrbdResource
{
    public static final String PROP_KEY_RES_NAME = "name";
    public static final String PROP_KEY_ROLE = "role";

    public static final String ROLE_LABEL_PRIMARY  = "Primary";
    public static final String ROLE_LABEL_SECONDARY = "Secondary";
    public static final String ROLE_LABEL_UNKNOWN  = "Unknown";

    public enum Role
    {
        PRIMARY(ROLE_LABEL_PRIMARY),
        SECONDARY(ROLE_LABEL_SECONDARY),
        UNKNOWN(ROLE_LABEL_UNKNOWN);

        private String roleLabel;

        private Role(String label)
        {
            roleLabel = label;
        }

        public static Role parseRole(String roleLabel)
        {
            Role result = Role.UNKNOWN;
            if (roleLabel.equals(ROLE_LABEL_PRIMARY))
            {
                result = Role.PRIMARY;
            }
            else
            if (roleLabel.equals(ROLE_LABEL_SECONDARY))
            {
                result = Role.SECONDARY;
            }
            return result;
        }

        @Override
        public String toString()
        {
            return roleLabel;
        }
    }

    protected final String resName;
    protected Role resRole;
    protected Map<String, DrbdConnection> connList;
    protected Map<Integer, DrbdVolume> volList;

    protected DrbdResource(String name)
    {
        resName = name;
        resRole = Role.UNKNOWN;
        connList = new TreeMap<>();
        volList = new TreeMap<>();
    }

    public String getName()
    {
        return resName;
    }

    public Role getRole()
    {
        return resRole;
    }

    protected static DrbdResource newFromProps(Map<String, String> props)
        throws EventsSourceException
    {
        String name = props.get(PROP_KEY_RES_NAME);
        if (name == null)
        {
            throw new EventsSourceException(
                "Create resource event without a resource name"
            );
        }

        return new DrbdResource(name);
    }

    protected void update(Map<String, String> props, ResourceObserver obs)
    {
        String roleLabel = props.get(PROP_KEY_ROLE);
        if (roleLabel != null)
        {
            Role prevResRole = resRole;
            resRole = Role.parseRole(roleLabel);
            if (prevResRole != resRole)
            {
                obs.roleChanged(this, prevResRole, resRole);
            }
        }
    }
}
