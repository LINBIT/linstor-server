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
public interface VolumeDefinition extends TransactionObject
{
    public UUID getUuid();

    public ResourceDefinition getResourceDefinition();

    public VolumeNumber getVolumeNumber();

    public MinorNumber getMinorNr(AccessContext accCtx)
        throws AccessDeniedException;

    public void setMinorNr(AccessContext accCtx, MinorNumber newMinorNr)
        throws AccessDeniedException, SQLException;

    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException;

    public void setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, SQLException;

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    public StateFlags<VlmDfnFlags> getFlags();

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public VlmDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    public enum VlmDfnFlags implements Flags
    {
        DELETE(1L);

        public final long flagValue;

        private VlmDfnFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static VlmDfnFlags[] valuesOfIgnoreCase(String string)
        {
            VlmDfnFlags[] flags;
            if (string == null)
            {
                flags = new VlmDfnFlags[0];
            }
            else
            {
                String[] split = string.split(",");
                flags = new VlmDfnFlags[split.length];

                for (int i = 0; i < split.length; i++)
                {
                    flags[i] = VlmDfnFlags.valueOf(split[i].toUpperCase().trim());
                }
            }
            return flags;
        }

        public static VlmDfnFlags[] restoreFlags(long vlmDfnFlags)
        {
            List<VlmDfnFlags> flagList = new ArrayList<>();
            for (VlmDfnFlags flag : VlmDfnFlags.values())
            {
                if ((vlmDfnFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new VlmDfnFlags[flagList.size()]);
        }
    }

    public interface VlmDfnApi
    {
        UUID getUuid();
        Integer getVolumeNr();
        Integer getMinorNr();
        long getSize();
        long getFlags();
        Map<String, String> getProps();
    }
}
