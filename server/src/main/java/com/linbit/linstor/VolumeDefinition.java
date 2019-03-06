package com.linbit.linstor;

import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.utils.Pair;

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
public interface VolumeDefinition extends TransactionObject, DbgInstanceUuid, Comparable<VolumeDefinition>
{
    UUID getUuid();

    ResourceDefinition getResourceDefinition();

    VolumeNumber getVolumeNumber();

    long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException;

    Long setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, SQLException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    StateFlags<VlmDfnFlags> getFlags();

    Iterator<Volume> iterateVolumes(AccessContext accCtx)
        throws AccessDeniedException;

    Stream<Volume> streamVolumes(AccessContext accCtx)
        throws AccessDeniedException;

    void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    boolean isDeleted();

    VlmDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    void setCryptKey(AccessContext accCtx, String key) throws AccessDeniedException, SQLException;

    String getCryptKey(AccessContext accCtx) throws AccessDeniedException;

    <T extends VlmDfnLayerObject> T setLayerData(AccessContext accCtx, T layerData)
        throws AccessDeniedException;

    <T extends VlmDfnLayerObject> T getLayerData(AccessContext accCtx, DeviceLayerKind kind)
        throws AccessDeniedException;

    @Override
    default int compareTo(VolumeDefinition otherVlmDfn)
    {
        int eq = getResourceDefinition().compareTo(otherVlmDfn.getResourceDefinition());
        if (eq == 0)
        {
            eq = getVolumeNumber().compareTo(otherVlmDfn.getVolumeNumber());
        }
        return eq;
    }

    enum VlmDfnFlags implements Flags
    {
        DELETE(1L),
        ENCRYPTED(2L),
        RESIZE(4L);

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
        long getSize();
        long getFlags();
        Map<String, String> getProps();
        List<Pair<String, VlmDfnLayerDataApi>> getVlmDfnLayerData();
    }

    public interface VlmDfnWtihCreationPayload
    {
        VlmDfnApi getVlmDfn();
        Integer getDrbdMinorNr();
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

        public Key(Volume.Key vlmKey)
        {
            rscName = vlmKey.getResourceName();
            vlmNr = vlmKey.getVolumeNumber();
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
            Key key = (Key) o;
            return Objects.equals(rscName, key.rscName) &&
                Objects.equals(vlmNr, key.vlmNr);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rscName, vlmNr);
        }
    }

    public interface InitMaps
    {
        Map<String, Volume> getVlmMap();
    }
}
