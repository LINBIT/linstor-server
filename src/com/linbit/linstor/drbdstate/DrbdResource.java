package com.linbit.linstor.drbdstate;

import com.linbit.ImplementationError;
import com.linbit.linstor.VolumeNumber;

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
    private final Map<String, DrbdConnection> connList;
    private final Map<VolumeNumber, DrbdVolume> volList;

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

    public DrbdConnection getConnection(String name)
    {
        if (name == null)
        {
            throw new ImplementationError(
                "Attempt to obtain a DrbdConnection object with name == null",
                new NullPointerException()
            );
        }
        DrbdConnection conn = null;
        synchronized (connList)
        {
            conn = connList.get(name);
        }
        return conn;
    }

    void putConnection(DrbdConnection connection)
    {
        synchronized (connList)
        {
            connList.put(connection.peerName, connection);
        }
    }

    DrbdConnection removeConnection(String name)
    {
        // Map.remove(null) is a valid operation, avoid hiding bugs
        if (name == null)
        {
            throw new ImplementationError(
                "Attempt to remove a DrbdResource object with name == null",
                new NullPointerException()
            );
        }
        DrbdConnection removedConn = null;
        synchronized (connList)
        {
            removedConn = connList.remove(name);
        }
        return removedConn;
    }

    public DrbdVolume getVolume(VolumeNumber volNr)
    {
        DrbdVolume vol = null;
        synchronized (volList)
        {
            vol = volList.get(volNr);
        }
        return vol;
    }

    void putVolume(DrbdVolume volume)
    {
        synchronized (volList)
        {
            volList.put(volume.volId, volume);
        }
    }

    DrbdVolume removeVolume(VolumeNumber volNr)
    {
        DrbdVolume removedVol = null;
        synchronized (volList)
        {
            removedVol = volList.remove(volNr);
        }
        return removedVol;
    }
}
