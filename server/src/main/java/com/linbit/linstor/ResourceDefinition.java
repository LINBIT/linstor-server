package com.linbit.linstor;

import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.utils.Pair;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ResourceDefinition
    extends TransactionObject, DbgInstanceUuid, Comparable<ResourceDefinition>, ProtectedObject
{
    UUID getUuid();

    ResourceName getName();

    byte[] getExternalName();

    int getVolumeDfnCount(AccessContext accCtx)
        throws AccessDeniedException;

    VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException;

    Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException;

    Stream<VolumeDefinition> streamVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException;

    int getResourceCount();

    int diskfullCount(AccessContext accCtx)
        throws AccessDeniedException;

    Iterator<Resource> iterateResource(AccessContext accCtx)
        throws AccessDeniedException;

    Stream<Resource> streamResource(AccessContext accCtx)
        throws AccessDeniedException;

    void copyResourceMap(
        AccessContext accCtx, Map<NodeName, ? super Resource> dstMap
    )
        throws AccessDeniedException;

    Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException;

    void addResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    void addSnapshotDfn(AccessContext accCtx, SnapshotDefinition snapshotDfn)
        throws AccessDeniedException;

    SnapshotDefinition getSnapshotDfn(AccessContext accCtx, SnapshotName snapshotName)
        throws AccessDeniedException;

    Collection<SnapshotDefinition> getSnapshotDfns(AccessContext accCtx)
        throws AccessDeniedException;

    void removeSnapshotDfn(AccessContext accCtx, SnapshotName snapshotName)
        throws AccessDeniedException;

    boolean hasDiskless(AccessContext accCtx)
        throws AccessDeniedException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    StateFlags<RscDfnFlags> getFlags();

    List<DeviceLayerKind> getLayerStack(AccessContext accCtx)
        throws AccessDeniedException;

    void setLayerStack(AccessContext accCtx, List<DeviceLayerKind> list)
        throws AccessDeniedException;

    void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    /**
     * Checks if any resource in the definition is currently used (mounted).
     * Returns an Optional<Resource> object containing the resources that is mounted or an empty.
     *
     * @param accCtx AccessContext for checks
     * @return The first found mounted/primary resource, if none is mounted returns empty optional.
     * @throws AccessDeniedException
     */
    Optional<Resource> anyResourceInUse(AccessContext accCtx)
        throws AccessDeniedException;

    RscDfnApi getApiData(AccessContext accCtx)
        throws AccessDeniedException;

    <T extends RscDfnLayerObject> T setLayerData(AccessContext accCtx, T layerData)
        throws AccessDeniedException, SQLException;

    /**
     * Returns a map of <ResourceNameSuffix, RscDfnLayerObject> where the RscDfnLayerObject has
     * the same DeviceLayerKind as the given argument
     * @throws AccessDeniedException
     */
    <T extends RscDfnLayerObject> Map<String, T> getLayerData(
        AccessContext accessContextRef,
        DeviceLayerKind kind
    )
        throws AccessDeniedException;

    /**
     * Returns a single RscDfnLayerObject matching the kind as well as the resourceNameSuffix.
     */
    <T extends RscDfnLayerObject> T getLayerData(
        AccessContext accCtx,
        DeviceLayerKind kind,
        String rscNameSuffix
    )
        throws AccessDeniedException;

    @Override
    default int compareTo(ResourceDefinition otherRscDfn)
    {
        return getName().compareTo(otherRscDfn.getName());
    }

    enum RscDfnFlags implements Flags
    {
        DELETE(1L);

        public final long flagValue;

        RscDfnFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static RscDfnFlags[] restoreFlags(long rawFlags)
        {
            List<RscDfnFlags> list = new ArrayList<>();
            for (RscDfnFlags flag : RscDfnFlags.values())
            {
                if ((rawFlags & flag.flagValue) == flag.flagValue)
                {
                    list.add(flag);
                }
            }
            return list.toArray(new RscDfnFlags[list.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(RscDfnFlags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(RscDfnFlags.class, listFlags);
        }
    }

    enum TransportType
    {
        IP, RDMA, RoCE;

        public static TransportType byValue(String str)
        {
            TransportType type = null;
            switch (str.toUpperCase())
            {
                case "IP":
                    type = IP;
                    break;
                case "RDMA":
                    type = RDMA;
                    break;
                case "ROCE":
                    type = RoCE;
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Unknown TransportType: '" + str + "'"
                    );
            }
            return type;
        }

        public static TransportType valueOfIgnoreCase(String string, TransportType defaultValue)
            throws IllegalArgumentException
        {
            TransportType ret = defaultValue;
            if (string != null)
            {
                TransportType val = valueOf(string.toUpperCase());
                if (val != null)
                {
                    ret = val;
                }
            }
            return ret;
        }
    }

    public interface RscDfnApi
    {
        UUID getUuid();
        String getResourceName();
        byte[] getExternalName();
        long getFlags();
        Map<String, String> getProps();
        List<VolumeDefinition.VlmDfnApi> getVlmDfnList();
        List<Pair<String, RscDfnLayerDataApi>> getLayerData();
    }

    public interface InitMaps
    {
        Map<NodeName, Resource> getRscMap();
        Map<VolumeNumber, VolumeDefinition> getVlmDfnMap();
        Map<SnapshotName, SnapshotDefinition> getSnapshotDfnMap();
    }
}
