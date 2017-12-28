package com.linbit.linstor;

import com.linbit.TransactionObject;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
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
public interface Volume extends TransactionObject
{
    public UUID getUuid();

    public Resource getResource();

    public ResourceDefinition getResourceDefinition();

    public VolumeDefinition getVolumeDefinition();

    public Props getProps(AccessContext accCtx) throws AccessDeniedException;

    public StateFlags<VlmFlags> getFlags();

    public VolumeConnection getVolumeConnection(AccessContext dbCtx, Volume otherVol)
        throws AccessDeniedException;

    public void setVolumeConnection(AccessContext accCtx, VolumeConnectionData volumeConnection)
        throws AccessDeniedException;

    public void removeVolumeConnection(AccessContext accCtx, VolumeConnectionData volumeConnection)
        throws AccessDeniedException;

    public StorPool getStorPool(AccessContext accCtx) throws AccessDeniedException;

    public String getBlockDevicePath(AccessContext accCtx) throws AccessDeniedException;

    public String getMetaDiskPath(AccessContext accCtx) throws AccessDeniedException;

    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;

    public enum VlmFlags implements Flags
    {
        CLEAN(1L),
        DELETE(2L);

        public final long flagValue;

        private VlmFlags(long value)
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
    }

    public VlmApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    public interface VlmApi
    {
        public UUID getVlmUuid();
        public UUID getVlmDfnUuid();
        public String getStorPoolName();
        public UUID getStorPoolUuid();
        public String getBlockDevice();
        public String getMetaDisk();
        public int getVlmNr();
        public long getFlags();
        public Map<String, String> getVlmProps();
    }

}
