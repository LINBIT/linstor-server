package com.linbit.linstor.core.objects;

import com.linbit.Checks;
import com.linbit.ErrorCheck;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeDefinition extends AbsCoreObj<VolumeDefinition> implements ProtectedObject
{
    public interface InitMaps
    {
        Map<String, Volume> getVlmMap();
    }

    private static final long DFLT_MIN_VLM_SIZE = 1;

    private static final long DFLT_MAX_VLM_SIZE = Long.MAX_VALUE;

    // Resource definition this VolumeDefinition belongs to
    private final ResourceDefinition resourceDfn;

    // DRBD volume number
    private final VolumeNumber volumeNr;

    // Net volume size in kiB
    private final TransactionSimpleObject<VolumeDefinition, Long> volumeSize;

    // Properties container for this volume definition
    private final Props vlmDfnProps;

    // State flags
    private final StateFlags<VolumeDefinition.Flags> flags;

    private final TransactionMap<VolumeDefinition, String, Volume> volumes;

    private final VolumeDefinitionDatabaseDriver dbDriver;

    private transient TransactionSimpleObject<VolumeDefinition, String> cryptKey;

    private final TransactionMap<VolumeDefinition, PairNonNull<DeviceLayerKind, String>, VlmDfnLayerObject> layerStorage;

    private final Key vlmDfnKey;

    VolumeDefinition(
        UUID uuid,
        ResourceDefinition resDfnRef,
        VolumeNumber volNr,
        long volSize,
        long initFlags,
        VolumeDefinitionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<String, Volume> vlmMapRef,
        Map<PairNonNull<DeviceLayerKind, String>, VlmDfnLayerObject> layerDataMapRef
    )
        throws MdException, DatabaseException
    {
        super(uuid, transObjFactory, transMgrProviderRef);
        ErrorCheck.ctorNotNull(VolumeDefinition.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(VolumeDefinition.class, VolumeNumber.class, volNr);

        checkVolumeSize(volSize, layerDataMapRef);

        resourceDfn = resDfnRef;

        dbDriver = dbDriverRef;

        volumeNr = volNr;
        volumeSize = transObjFactory.createTransactionSimpleObject(
            this,
            volSize,
            dbDriver.getVolumeSizeDriver()
        );
        vlmDfnKey = new Key(this);

        vlmDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resDfnRef.getName(), volumeNr),
            toStringImpl(),
            LinStorObject.VLM_DFN
        );

        layerStorage = transObjFactory.createTransactionMap(this, layerDataMapRef, null);

        volumes = transObjFactory.createTransactionMap(this, vlmMapRef, null);

        flags = transObjFactory.createStateFlagsImpl(
            resDfnRef.getObjProt(),
            this,
            VolumeDefinition.Flags.class,
            this.dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        cryptKey = transObjFactory.createTransactionSimpleObject(this, null, null);

        transObjs = Arrays.asList(
            vlmDfnProps,
            resourceDfn,
            volumeSize,
            flags,
            layerStorage,
            deleted,
            cryptKey
        );

    }

    public void recheckVolumeSize(AccessContext accCtxRef)
        throws AccessDeniedException, MinSizeException, MaxSizeException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtxRef, AccessType.VIEW);
        checkVolumeSize(volumeSize.get(), layerStorage);
    }

    static void checkVolumeSize(
        long volSize,
        Map<PairNonNull<DeviceLayerKind, String>, VlmDfnLayerObject> layerStorageRef
    )
        throws MinSizeException, MaxSizeException
    {
        long minValue = DFLT_MIN_VLM_SIZE;
        long maxValue = DFLT_MAX_VLM_SIZE;
        String errMsgAppendix = "for non-DRBD";
        try
        {
            // only check if we already have at least one DRBD resource deployed
            boolean hasDrbd = false;
            for (VlmDfnLayerObject vlmDfnLO : layerStorageRef.values())
            {
                if (vlmDfnLO instanceof DrbdVlmDfnData)
                {
                    hasDrbd = true;
                    break;
                }
            }

            if (hasDrbd)
            {
                minValue = MetaData.DRBD_MIN_NET_kiB;
                maxValue = MetaData.DRBD_MAX_kiB;
                errMsgAppendix = "for DRBD";
            }
            Checks.genericRangeCheck(
                volSize,
                minValue,
                maxValue,
                "Volume size value %d is out of range [%d - %d]"
            );
        }
        catch (ValueOutOfRangeException valueExc)
        {
            String excMessage = String.format(
                "Volume size value %d is out of range [%d - %d] %s",
                volSize,
                minValue,
                maxValue,
                errMsgAppendix
            );
            if (valueExc.getViolationType() == ValueOutOfRangeException.ViolationType.TOO_LOW)
            {
                throw new MinSizeException(excMessage);
            }
            throw new MaxSizeException(excMessage);
        }
    }

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, resourceDfn.getObjProt(), vlmDfnProps);
    }

    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    public VolumeNumber getVolumeNumber()
    {
        checkDeleted();
        return volumeNr;
    }

    public Key getKey()
    {
        // no check deleted
        return vlmDfnKey;
    }

    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeSize.get();
    }

    public Long setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, DatabaseException, MinSizeException, MaxSizeException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        checkVolumeSize(newVolumeSize, layerStorage);
        return volumeSize.set(newVolumeSize);
    }

    public StateFlags<VolumeDefinition.Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public void putVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumes.put(Resource.getStringId(volume.getAbsResource()), volume);
    }

    public void removeVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumes.remove(Resource.getStringId(volume.getAbsResource()));
    }

    public Iterator<Volume> iterateVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumes.values().iterator();
    }

    public Stream<Volume> streamVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumes.values().stream();
    }

    public void setCryptKey(AccessContext accCtx, String key) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        if (!accCtx.subjectId.equals(Identity.SYSTEM_ID))
        {
            throw new AccessDeniedException("Only system context is allowed to set crypt key");
        }
        cryptKey.set(key);
    }

    public String getCryptKey(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        if (!accCtx.subjectId.equals(Identity.SYSTEM_ID))
        {
            throw new AccessDeniedException("Only system context is allowed to get crypt key");
        }
        return cryptKey.get();
    }


    @SuppressWarnings("unchecked")
    public <T extends VlmDfnLayerObject> T setLayerData(AccessContext accCtx, T vlmDfnLayerData)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        return (T) layerStorage.put(
            new PairNonNull<>(
                vlmDfnLayerData.getLayerKind(),
                vlmDfnLayerData.getRscNameSuffix()
            ), vlmDfnLayerData
        );
    }

    @SuppressWarnings("unchecked")
    public <T extends VlmDfnLayerObject> Map<String, T> getLayerData(
        AccessContext accCtx,
        DeviceLayerKind kind
    )
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        Map<String, T> ret = new TreeMap<>();
        for (Entry<PairNonNull<DeviceLayerKind, String>, VlmDfnLayerObject> entry : layerStorage.entrySet())
        {
            PairNonNull<DeviceLayerKind, String> key = entry.getKey();
            if (key.objA.equals(kind))
            {
                ret.put(key.objB, (T) entry.getValue());
            }
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public <T extends VlmDfnLayerObject> @Nullable T getLayerData(
        AccessContext accCtx,
        DeviceLayerKind kind,
        String rscNameSuffix
    )
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        return (T) layerStorage.get(new PairNonNull<>(kind, rscNameSuffix));
    }

    public void removeLayerData(AccessContext accCtx, DeviceLayerKind kind, String rscNameSuffix)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        layerStorage.remove(new PairNonNull<>(kind, rscNameSuffix)).delete();
    }

    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);
        getFlags().enableFlags(accCtx, VolumeDefinition.Flags.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            resourceDfn.removeVolumeDefinition(accCtx, this);

            // preventing ConcurrentModificationException
            List<Volume> vlms = new ArrayList<>(volumes.values());
            for (Volume vlm : vlms)
            {
                vlm.delete(accCtx);
            }

            vlmDfnProps.delete();

            for (VlmDfnLayerObject vlmDfnLayerObject : layerStorage.values())
            {
                vlmDfnLayerObject.delete();
            }

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    public VolumeDefinitionApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        List<Pair<String, VlmDfnLayerDataApi>> layerData = new ArrayList<>();

        /*
         * Satellite should not care about this layerStack (and especially not about its ordering)
         * as the satellite should only take the resource's tree-structured layerData into account
         *
         * This means, this serialization is basically only for clients.
         *
         * Sorting an enum by default orders by its ordinal number, not alphanumerically.
         */

        TreeSet<PairNonNull<DeviceLayerKind, String>> sortedLayerStack = new TreeSet<>();
        for (DeviceLayerKind kind : resourceDfn.getLayerStack(accCtx))
        {
            sortedLayerStack.add(new PairNonNull<>(kind, ""));
        }

        sortedLayerStack.addAll(layerStorage.keySet());

        for (PairNonNull<DeviceLayerKind, String> pair : sortedLayerStack)
        {
            VlmDfnLayerObject vlmDfnLayerObject = layerStorage.get(pair);
            layerData.add(
                new Pair<>(
                    pair.objA.name(),
                    vlmDfnLayerObject == null ? null : vlmDfnLayerObject.getApiData(accCtx)
                )
            );
        }

        return new VlmDfnPojo(
            getUuid(),
            getVolumeNumber().value,
            getVolumeSize(accCtx),
            getFlags().getFlagsBits(accCtx),
            getProps(accCtx).map(),
            layerData
        );
    }

    @Override
    public String toStringImpl()
    {
        return "RscName: " + vlmDfnKey.rscName +
               ", VlmNr: '" + volumeNr + "'";
    }

    @Override
    public int compareTo(VolumeDefinition otherVlmDfn)
    {
        int eq = getResourceDefinition().compareTo(otherVlmDfn.getResourceDefinition());
        if (eq == 0)
        {
            eq = getVolumeNumber().compareTo(otherVlmDfn.getVolumeNumber());
        }
        return eq;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(resourceDfn, volumeNr);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof VolumeDefinition)
        {
            VolumeDefinition other = (VolumeDefinition) obj;
            other.checkDeleted();
            ret = Objects.equals(resourceDfn, other.resourceDfn) && Objects.equals(volumeNr, other.volumeNr);
        }
        return ret;
    }

    /**
     * Sortable key for sets of volumes. Sorts by resource name, then volume number.
     */
    public static class Key implements Comparable<Key>
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
            rscName = rscRef.getResourceDefinition().getName();
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
        @SuppressWarnings(
            {
                "DescendantToken", "ParameterName"
            }
        )
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

    @SuppressWarnings("checkstyle:MagicNumber")
    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L),
        ENCRYPTED(1L << 1),
        RESIZE(1L << 2),
        GROSS_SIZE(1L << 3),
        RESIZE_SHRINK(RESIZE.getFlagValue() | 1L << 4);

        public final long flagValue;

        Flags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static Flags[] valuesOfIgnoreCase(String string)
        {
            Flags[] flags;
            if (string == null)
            {
                flags = new Flags[0];
            }
            else
            {
                String[] split = string.split(",");
                flags = new Flags[split.length];

                for (int idx = 0; idx < split.length; idx++)
                {
                    flags[idx] = Flags.valueOf(split[idx].toUpperCase().trim());
                }
            }
            return flags;
        }

        public static Flags[] restoreFlags(long vlmDfnFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((vlmDfnFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new Flags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(Flags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(Flags.class, listFlags);
        }
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return resourceDfn.getObjProt();
    }
}
