package com.linbit.linstor;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.utils.RemoveAfterDevMgrRework;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Resource extends TransactionObject, DbgInstanceUuid, Comparable<Resource>, ProtectedObject
{
    UUID getUuid();

    ResourceDefinition getDefinition();

    Volume getVolume(VolumeNumber volNr);

    int getVolumeCount();

    Iterator<Volume> iterateVolumes();

    Stream<Volume> streamVolumes();

    Node getAssignedNode();

    @RemoveAfterDevMgrRework
    NodeId getNodeId();

    Stream<ResourceConnection> streamResourceConnections(AccessContext accCtx)
        throws AccessDeniedException;

    ResourceConnection getResourceConnection(AccessContext accCtx, Resource otherResource)
        throws AccessDeniedException;

    void setResourceConnection(AccessContext accCtx, ResourceConnection resCon)
        throws AccessDeniedException;

    void removeResourceConnection(AccessContext accCtx, ResourceConnection resCon)
        throws AccessDeniedException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    StateFlags<RscFlags> getStateFlags();

    /**
     * Whether peers should treat this resource as diskless.
     */
    @RemoveAfterDevMgrRework
    boolean disklessForPeers(AccessContext accCtx)
        throws AccessDeniedException;

    void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    boolean isDeleted();

    @RemoveAfterDevMgrRework
    boolean supportsDrbd(AccessContext accCtx)
        throws AccessDeniedException;

    RscApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException;

    @RemoveAfterDevMgrRework
    boolean isCreatePrimary();

    boolean isDiskless(AccessContext accCtx)
        throws AccessDeniedException;

    <T extends RscLayerObject> T getLayerData(AccessContext accCtx, DeviceLayerKind kind)
        throws AccessDeniedException;

    void setLayerData(AccessContext accCtx, RscLayerObject layerData)
        throws AccessDeniedException;

    /**
     * Returns the identification key without checking if "this" is already deleted
     */
    Key getKey();

    @Override
    default int compareTo(Resource otherRsc)
    {
        int eq = getAssignedNode().compareTo(otherRsc.getAssignedNode());
        if (eq == 0)
        {
            eq = getDefinition().compareTo(otherRsc.getDefinition());
        }
        return eq;
    }

    static String getStringId(Resource rsc)
    {
        return rsc.getAssignedNode().getName().value + "/" +
               rsc.getDefinition().getName().value;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    enum RscFlags implements Flags
    {
        CLEAN(1L << 0),
        DELETE(1L << 1),
        DISKLESS(1L << 2),
        DISK_ADD_REQUESTED(1L << 3),
        DISK_ADDING(1L << 4),
        DISK_REMOVE_REQUESTED(1L << 5),
        DISK_REMOVING(1L << 6);

        public final long flagValue;

        RscFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static RscFlags[] restoreFlags(long rscFlags)
        {
            List<RscFlags> flagList = new ArrayList<>();
            for (RscFlags flag : RscFlags.values())
            {
                if ((rscFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new RscFlags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(RscFlags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(RscFlags.class, listFlags);
        }
    }

    public interface RscApi
    {
        UUID getUuid();
        String getName();
        UUID getNodeUuid();
        String getNodeName();
        UUID getRscDfnUuid();
        Map<String, String> getProps();
        long getFlags();
        List<? extends Volume.VlmApi> getVlmList();
        Integer getLocalRscNodeId();
    }

    /**
     * Identifies a resource globally.
     */
    class Key implements Comparable<Key>
    {
        private final NodeName nodeName;
        private final ResourceName resourceName;

        public Key(Resource resource)
        {
            this(resource.getAssignedNode().getName(), resource.getDefinition().getName());
        }

        public Key(NodeName nodeNameRef, ResourceName resourceNameRef)
        {
            resourceName = resourceNameRef;
            nodeName = nodeNameRef;
        }

        public NodeName getNodeName()
        {
            return nodeName;
        }

        public ResourceName getResourceName()
        {
            return resourceName;
        }

        @Override
        // Code style exception: Automatically generated code
        @SuppressWarnings({"DescendantToken", "ParameterName"})
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            Key that = (Key) o;
            return Objects.equals(nodeName, that.nodeName) &&
                Objects.equals(resourceName, that.resourceName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeName, resourceName);
        }

        @Override
        public int compareTo(Key other)
        {
            int eq = nodeName.compareTo(other.nodeName);
            if (eq == 0)
            {
                eq = resourceName.compareTo(other.resourceName);
            }
            return eq;
        }

        @Override
        public String toString()
        {
            return "Resource.Key [Node: " + nodeName + ", Resource: " + resourceName + "]";
        }
    }

    public interface InitMaps
    {
        Map<Resource.Key, ResourceConnection> getRscConnMap();
        Map<VolumeNumber, Volume> getVlmMap();
    }
}
