package com.linbit.linstor;

import com.linbit.TransactionObject;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Volume extends TransactionObject, DbgInstanceUuid
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

    String getBlockDevicePath(AccessContext accCtx) throws AccessDeniedException;

    String getMetaDiskPath(AccessContext accCtx) throws AccessDeniedException;

    void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException;

    void setBlockDevicePath(AccessContext accCtx, String path) throws AccessDeniedException;

    void setMetaDiskPath(AccessContext accCtx, String path) throws AccessDeniedException;

    void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;

    enum VlmFlags implements Flags
    {
        CLEAN(1L),
        DELETE(2L);

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
        String getBlockDevice();
        String getMetaDisk();
        int getVlmNr();
        int getVlmMinorNr();
        long getFlags();
        Map<String, String> getVlmProps();
    }
}
