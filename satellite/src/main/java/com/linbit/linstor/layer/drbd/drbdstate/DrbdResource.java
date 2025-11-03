package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
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
    public static final String PROP_KEY_SUSPENDED = "suspended";
    public static final String PROP_KEY_MAY_PROMOTE = "may_promote";
    public static final String PROP_KEY_PROMOTION_SCORE = "promotion_score";

    public static final String ROLE_LABEL_PRIMARY  = "Primary";
    public static final String ROLE_LABEL_SECONDARY = "Secondary";
    public static final String ROLE_LABEL_UNKNOWN  = "Unknown";

    private static final String SUSPENDED_LABEL_USER = "user";

    public enum Role
    {
        PRIMARY(ROLE_LABEL_PRIMARY),
        SECONDARY(ROLE_LABEL_SECONDARY),
        UNKNOWN(ROLE_LABEL_UNKNOWN);

        private final String roleLabel;

        Role(String label)
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

    protected final @Nullable ResourceName resName;
    protected final String resNameStr;
    protected Role resRole;
    protected @Nullable Boolean suspendedUser;
    protected @Nullable Boolean mayPromote;
    protected @Nullable Integer promotionScore;
    private final Map<String, DrbdConnection> connList;
    private final Map<VolumeNumber, DrbdVolume> volList;
    protected boolean isLinstorDrbdResource;
    protected boolean suppressDestroyEvent;
    protected boolean suppressRscEventsWhenPrimary;

    protected DrbdResource(String nameStr)
    {
        resName = asRscName(nameStr);
        resNameStr = nameStr;
        resRole = Role.UNKNOWN;
        suspendedUser = null;
        connList = new TreeMap<>();
        volList = new TreeMap<>();
    }

    private @Nullable ResourceName asRscName(String nameStr)
    {
        ResourceName rscName = null;
        try
        {
            rscName = new ResourceName(nameStr);
        }
        catch (InvalidNameException ignored)
        {
        }
        return rscName;
    }

    public @Nullable ResourceName getResName()
    {
        return resName;
    }

    public String getNameString()
    {
        return resNameStr;
    }

    public Role getRole()
    {
        return resRole;
    }

    public @Nullable Boolean getSuspendedUser()
    {
        return suspendedUser;
    }

    public @Nullable Integer getPromotionScore()
    {
        return promotionScore;
    }

    public @Nullable Boolean mayPromote()
    {
        return mayPromote;
    }

    public boolean isDestroyEventSuppressed()
    {
        return suppressDestroyEvent;
    }

    public void suppressDestroyEvent(final boolean flag)
    {
        suppressDestroyEvent = flag;
    }

    public boolean areRscEventsSuppressedWhenPrimary()
    {
        return suppressRscEventsWhenPrimary;
    }

    public void suppressRscEventsWhenPrimary(final boolean flag)
    {
        suppressRscEventsWhenPrimary = flag;
    }

    protected static DrbdResource newFromProps(
        Map<String, String> props,
        ResourceDefinitionMap rscDfnMap
    )
        throws EventsSourceException
    {
        String name = props.get(PROP_KEY_RES_NAME);
        if (name == null)
        {
            throw new EventsSourceException(
                "Create resource event without a resource name"
            );
        }
        DrbdResource drbdResource = new DrbdResource(name);
        if (drbdResource.resName != null)
        {
            ResourceDefinition rscDfn = rscDfnMap.get(drbdResource.resName);
            if (rscDfn != null)
            {
                drbdResource.setKnownByLinstor(
                    rscDfn.getName().displayValue
                        .equals(name)
                );
            }
        }
        return drbdResource;
    }

    protected void update(Map<String, String> props, ResourceObserver obs)
        throws EventsSourceException
    {
        String roleLabel = props.get(PROP_KEY_ROLE);
        String suspendedLabel = props.get(PROP_KEY_SUSPENDED);
        String mayPromoteLabel = props.get(PROP_KEY_MAY_PROMOTE);
        String promotionScoreLabel = props.get(PROP_KEY_PROMOTION_SCORE);

        if (roleLabel != null)
        {
            Role prevResRole = resRole;
            resRole = Role.parseRole(roleLabel);
            if (prevResRole != resRole)
            {
                obs.roleChanged(this, prevResRole, resRole);
            }
        }

        if (suspendedLabel != null)
        {
            suspendedUser = Arrays.asList(suspendedLabel.split(",")).contains(SUSPENDED_LABEL_USER);
        }

        if (mayPromoteLabel != null)
        {
            final Boolean prevMayPromote = mayPromote;
            if (mayPromoteLabel.equals("yes"))
            {
                mayPromote = true;
            }
            else
            if (mayPromoteLabel.equals("no"))
            {
                mayPromote = false;
            }
            else
            {
                mayPromote = null;
            }
            if (!Objects.equals(prevMayPromote, mayPromote))
            {
                obs.mayPromoteChanged(this, prevMayPromote, mayPromote);
            }
        }

        if (promotionScoreLabel != null)
        {
            final Integer prevPromotionScore = promotionScore;
            try
            {
                promotionScore = Integer.parseInt(promotionScoreLabel);
            }
            catch (NumberFormatException nfExc)
            {
                promotionScore = null;
                throw new EventsSourceException(
                    "Event line with unparsable " + PROP_KEY_PROMOTION_SCORE + " number"
                );
            }
            if (!Objects.equals(prevPromotionScore, promotionScore))
            {
                obs.promotionScoreChanged(this, prevPromotionScore, promotionScore);
            }
        }
    }

    public @Nullable DrbdConnection getConnection(String name)
    {
        if (name == null)
        {
            throw new ImplementationError(
                "Attempt to obtain a DrbdConnection object with name == null",
                new NullPointerException()
            );
        }
        DrbdConnection conn;
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

    @Nullable
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
        DrbdConnection removedConn;
        synchronized (connList)
        {
            removedConn = connList.remove(name);
        }
        return removedConn;
    }

    public Map<String, DrbdConnection> getConnectionsMap()
    {
        Map<String, DrbdConnection> connListCopy;
        synchronized (connList)
        {
            connListCopy = new TreeMap<>(connList);
        }
        return connListCopy;
    }

    public @Nullable DrbdVolume getVolume(VolumeNumber volNr)
    {
        DrbdVolume vol;
        synchronized (volList)
        {
            vol = volList.get(volNr);
        }
        return vol;
    }

    public Map<VolumeNumber, DrbdVolume> getVolumesMap()
    {
        Map<VolumeNumber, DrbdVolume> volListCopy = new TreeMap<>();
        synchronized (volList)
        {
            volListCopy.putAll(volList);
        }
        return volListCopy;
    }

    public Iterator<DrbdVolume> iterateVolumes()
    {
        LinkedList<DrbdVolume> volListCopy = new LinkedList<>();
        synchronized (volList)
        {
            volListCopy.addAll(volList.values());
        }
        return volListCopy.iterator();
    }

    void putVolume(DrbdVolume volume)
    {
        synchronized (volList)
        {
            volList.put(volume.volId, volume);
        }
    }

    @Nullable
    DrbdVolume removeVolume(VolumeNumber volNr)
    {
        DrbdVolume removedVol;
        synchronized (volList)
        {
            removedVol = volList.remove(volNr);
        }
        return removedVol;
    }

    public boolean isKnownByLinstor()
    {
        return isLinstorDrbdResource;
    }

    public void setKnownByLinstor(boolean knownByLinstor)
    {
        isLinstorDrbdResource = knownByLinstor;
    }
}
