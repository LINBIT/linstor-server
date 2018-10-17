package com.linbit.linstor;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage2.layer.data.categories.RscLayerData;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.utils.RemoveAfterDevMgrRework;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
public interface Resource extends TransactionObject, DbgInstanceUuid, Comparable<Resource>
{
    UUID getUuid();

    ObjectProtection getObjProt();

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

    ResourceType getType();

    RscLayerData setLayerData(AccessContext accCtx, RscLayerData rscLayerData)
        throws AccessDeniedException, SQLException;

    RscLayerData getLayerData(AccessContext accCtx)
        throws AccessDeniedException;

    @Nullable Resource getParentResource(AccessContext accCtx)
        throws AccessDeniedException;

    void addChild(AccessContext accCtx, Resource child)
        throws AccessDeniedException, SQLException;

    void removeChild(AccessContext accCtx, Resource child)
        throws AccessDeniedException, SQLException;

    /**
     * @return A list that is not allowed to be null, but empty
     */
    @Nonnull List<Resource> getChildResources(AccessContext accCtx)
        throws AccessDeniedException;

    default void setParentResource(AccessContext accCtx, Resource parent)
        throws AccessDeniedException, SQLException
    {
        setParentResource(accCtx, parent, false);
    }

    void setParentResource(AccessContext accCtx, Resource parent, boolean overrideOldParent)
        throws AccessDeniedException, SQLException;

    void removeParent(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

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
               rsc.getDefinition().getName().value + "/" +
               rsc.getType().name();
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
        private final ResourceType resourceType;

        public Key(Resource resource)
        {
            this(resource.getAssignedNode().getName(), resource.getDefinition().getName(), resource.getType());
        }

        public Key(NodeName nodeNameRef, ResourceName resourceNameRef, ResourceType resourceTypeRef)
        {
            resourceName = resourceNameRef;
            nodeName = nodeNameRef;
            resourceType = resourceTypeRef;
        }

        public NodeName getNodeName()
        {
            return nodeName;
        }

        public ResourceName getResourceName()
        {
            return resourceName;
        }

        public ResourceType getResourceType()
        {
            return resourceType;
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
                Objects.equals(resourceName, that.resourceName) &&
                Objects.equals(resourceType, that.resourceType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeName, resourceName, resourceType);
        }

        @Override
        public int compareTo(Key other)
        {
            int eq = nodeName.compareTo(other.nodeName);
            if (eq == 0)
            {
                eq = resourceName.compareTo(other.resourceName);
                if (eq == 0)
                {
                    eq = resourceType.compareTo(other.resourceType);
                }
            }
            return eq;
        }

        @Override
        public String toString()
        {
            return "Resource.Key [Node: " + nodeName + ", Resource: " + resourceName + ", Type: " + resourceType + "]";
        }
    }

    public interface InitMaps
    {
        Map<Resource.Key, ResourceConnection> getRscConnMap();
        Map<VolumeNumber, Volume> getVlmMap();
    }
}
