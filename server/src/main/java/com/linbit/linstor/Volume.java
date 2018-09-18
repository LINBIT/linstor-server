package com.linbit.linstor;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Volume extends TransactionObject, DbgInstanceUuid, Comparable<Volume>
{
    UUID getUuid();

    Resource getResource();

    ResourceDefinition getResourceDefinition();

    VolumeDefinition getVolumeDefinition();

    Props getProps(AccessContext accCtx) throws AccessDeniedException;

    StateFlags<VlmFlags> getFlags();

    VolumeConnection getVolumeConnection(AccessContext dbCtx, Volume otherVol)
        throws AccessDeniedException;

    void setVolumeConnection(AccessContext accCtx, VolumeConnection volumeConnection)
        throws AccessDeniedException;

    void removeVolumeConnection(AccessContext accCtx, VolumeConnection volumeConnection)
        throws AccessDeniedException;

    StorPool getStorPool(AccessContext accCtx) throws AccessDeniedException;

    void setStorPool(AccessContext accCtx, StorPool storPool)
        throws AccessDeniedException, SQLException;

    String getBackingDiskPath(AccessContext accCtx) throws AccessDeniedException;

    String getMetaDiskPath(AccessContext accCtx) throws AccessDeniedException;

    String getDevicePath(AccessContext accCtx) throws AccessDeniedException;

    void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException;

    void setBackingDiskPath(AccessContext accCtx, String path) throws AccessDeniedException;

    void setMetaDiskPath(AccessContext accCtx, String path) throws AccessDeniedException;

    void setDevicePath(AccessContext accCtx, String path) throws AccessDeniedException;

    boolean isNettoSizeSet(AccessContext accCtx) throws AccessDeniedException;

    void setNettoSize(AccessContext accCtx, long size) throws AccessDeniedException;

    long getNettoSize(AccessContext accCtx) throws AccessDeniedException;

    boolean isBruttoSizeSet(AccessContext accCtx) throws AccessDeniedException;

    void setBruttoSize(AccessContext accCtx, long size) throws AccessDeniedException;

    long getBruttoSize(AccessContext accCtx) throws AccessDeniedException;

    long getEstimatedSize(AccessContext accCtx) throws AccessDeniedException;

    void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;

    @Override
    default int compareTo(Volume otherVlm)
    {
        int eq = getResource().getAssignedNode().compareTo(
            otherVlm.getResource().getAssignedNode()
        );
        if (eq == 0)
        {
            eq = getVolumeDefinition().compareTo(otherVlm.getVolumeDefinition()); // also contains rscName comparison
        }
        return eq;
    }

    static String getVolumeKey(Volume volume)
    {
        NodeName nodeName = volume.getResource().getAssignedNode().getName();
        ResourceName rscName = volume.getResourceDefinition().getName();
        VolumeNumber volNr = volume.getVolumeDefinition().getVolumeNumber();
        return nodeName.value + "/" + rscName.value + "/" + volNr.value;
    }

    enum VlmFlags implements Flags
    {
        DELETE(2L),
        RESIZE(4L),
        DRBD_RESIZE(8L);

        public final long flagValue;

        VlmFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static VlmFlags[] restoreFlags(long vlmFlags)
        {
            List<VlmFlags> flagList = new ArrayList<>();
            for (VlmFlags flag : VlmFlags.values())
            {
                if ((vlmFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new VlmFlags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(VlmFlags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(VlmFlags.class, listFlags);
        }
    }

    VlmApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    interface VlmApi
    {
        UUID getVlmUuid();
        UUID getVlmDfnUuid();
        String getStorPoolName();
        UUID getStorPoolUuid();
        String getStorDriverSimpleClassName();
        String getBlockDevice();
        String getMetaDisk();
        String getDevicePath();
        int getVlmNr();
        int getVlmMinorNr();
        long getFlags();
        Map<String, String> getVlmProps();
        UUID getStorPoolDfnUuid();
        Map<String, String> getStorPoolDfnProps();
        Map<String, String> getStorPoolProps();
    }

    public interface InitMaps
    {
        Map<Volume, VolumeConnection> getVolumeConnections();
    }

}
