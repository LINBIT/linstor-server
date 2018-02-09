package com.linbit.linstor;

import com.linbit.TransactionObject;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlags;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface VolumeDefinition extends TransactionObject, DbgInstanceUuid
{
    UUID getUuid();

    ResourceDefinition getResourceDefinition();

    VolumeNumber getVolumeNumber();

    MinorNumber getMinorNr(AccessContext accCtx)
        throws AccessDeniedException;

    MinorNumber setMinorNr(AccessContext accCtx, MinorNumber newMinorNr)
        throws AccessDeniedException, SQLException;

    long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException;

    Long setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, SQLException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    StateFlags<VlmDfnFlags> getFlags();

    Iterator<Volume> iterateVolumes(AccessContext accCtx)
        throws AccessDeniedException;

    void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    VlmDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    enum VlmDfnFlags implements Flags
    {
        DELETE(1L);

        public final long flagValue;

        VlmDfnFlags(long value)
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

                for (int idx = 0; idx < split.length; idx++)
                {
                    flags[idx] = VlmDfnFlags.valueOf(split[idx].toUpperCase().trim());
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

    /**
     * Sortable key for sets of volumes. Sorts by resource name, then volume number.
     */
    class Key implements Comparable<Key>
    {
        public final ResourceName rscName;
        public final VolumeNumber vlmNr;

        public Key(ResourceName rscNameRef, VolumeNumber vlmNrRef)
        {
            rscName = rscNameRef;
            vlmNr = vlmNrRef;
        }

        public Key(Resource rscRef, VolumeNumber vlmNrRef)
        {
            rscName = rscRef.getDefinition().getName();
            vlmNr = vlmNrRef;
        }

        public Key(ResourceDefinition rscDfnRef, VolumeNumber vlmNrRef)
        {
            rscName = rscDfnRef.getName();
            vlmNr = vlmNrRef;
        }

        public Key(VolumeDefinition vlmDfn)
        {
            rscName = vlmDfn.getResourceDefinition().getName();
            vlmNr = vlmDfn.getVolumeNumber();
        }

        public Key(Volume vlm)
        {
            rscName = vlm.getResourceDefinition().getName();
            vlmNr = vlm.getVolumeDefinition().getVolumeNumber();
        }

        @Override
        public int compareTo(Key other)
        {
            int result = rscName.compareTo(other.rscName);
            if (result == 0)
            {
                result = vlmNr.compareTo(other.vlmNr);
            }
            return result;
        }
    }
}
