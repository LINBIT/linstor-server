package com.linbit.linstor.core.objects;

import com.linbit.Checks;
import com.linbit.ErrorCheck;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
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
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.utils.Pair;

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
public class VolumeDefinition extends BaseTransactionObject implements DbgInstanceUuid, Comparable<VolumeDefinition>
{
    public static interface InitMaps
    {
        Map<String, Volume> getVlmMap();
    }

    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

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

    private final TransactionMap<String, Volume> volumes;

    private final VolumeDefinitionDatabaseDriver dbDriver;

    private final TransactionSimpleObject<VolumeDefinition, Boolean> deleted;

    private transient TransactionSimpleObject<VolumeDefinition, String> cryptKey;

    private final TransactionMap<Pair<DeviceLayerKind, String>, VlmDfnLayerObject> layerStorage;

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
        Map<Pair<DeviceLayerKind, String>, VlmDfnLayerObject> layerDataMapRef
    )
        throws MdException, DatabaseException
    {
        super(transMgrProviderRef);
        ErrorCheck.ctorNotNull(VolumeDefinition.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(VolumeDefinition.class, VolumeNumber.class, volNr);

        checkVolumeSize(volSize);

        objId = uuid;
        dbgInstanceId = UUID.randomUUID();
        resourceDfn = resDfnRef;

        dbDriver = dbDriverRef;

        volumeNr = volNr;
        volumeSize = transObjFactory.createTransactionSimpleObject(
            this,
            volSize,
            dbDriver.getVolumeSizeDriver()
        );

        vlmDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resDfnRef.getName(), volumeNr)
        );

        layerStorage = transObjFactory.createTransactionMap(layerDataMapRef, null);

        volumes = transObjFactory.createTransactionMap(vlmMapRef, null);

        flags = transObjFactory.createStateFlagsImpl(
            resDfnRef.getObjProt(),
            this,
            VolumeDefinition.Flags.class,
            this.dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);
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

    static void checkVolumeSize(long volSize)
        throws MinSizeException, MaxSizeException
    {
        try
        {
            Checks.genericRangeCheck(
                volSize, MetaData.DRBD_MIN_NET_kiB, MetaData.DRBD_MAX_kiB,
                "Volume size value %d is out of range [%d - %d]"
            );
        }
        catch (ValueOutOfRangeException valueExc)
        {
            String excMessage = String.format(
                "Volume size value %d is out of range [%d - %d]",
                volSize, MetaData.DRBD_MIN_NET_kiB, MetaData.DRBD_MAX_kiB
            );
            if (valueExc.getViolationType() == ValueOutOfRangeException.ViolationType.TOO_LOW)
            {
                throw new MinSizeException(excMessage);
            }
            throw new MaxSizeException(excMessage);
        }
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
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

    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeSize.get();
    }

    public Long setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return volumeSize.set(newVolumeSize);
    }

    public StateFlags<VolumeDefinition.Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public void putVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumes.put(Resource.getStringId(volume.getAbsResource()), volume);
    }

    public void removeVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumes.remove(Resource.getStringId(volume.getAbsResource()));
    }

    public Iterator<Volume> iterateVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumes.values().iterator();
    }

    public Stream<Volume> streamVolumes(AccessContext accCtx) throws AccessDeniedException
    {
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
            new Pair<>(
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
        for (Entry<Pair<DeviceLayerKind, String>, VlmDfnLayerObject> entry : layerStorage.entrySet())
        {
            Pair<DeviceLayerKind, String> key = entry.getKey();
            if (key.objA.equals(kind))
            {
                ret.put(key.objB, (T) entry.getValue());
            }
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public <T extends VlmDfnLayerObject> T getLayerData(
        AccessContext accCtx,
        DeviceLayerKind kind,
        String rscNameSuffix
    )
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        return (T) layerStorage.get(new Pair<>(kind, rscNameSuffix));
    }

    public void removeLayerData(AccessContext accCtx, DeviceLayerKind kind, String rscNameSuffix)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        layerStorage.remove(new Pair<>(kind, rscNameSuffix)).delete();
    }

    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);
        getFlags().enableFlags(accCtx, VolumeDefinition.Flags.DELETE);
    }

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

    public boolean isDeleted()
    {
        return deleted.get();
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted volume definition");
        }
    }

    public VolumeDefinitionApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        List<Pair<String, VlmDfnLayerDataApi>> layerData = new ArrayList<>();

        /*
         * Satellite should not care about this layerStack (and especially not about its ordering)
         * as the satellite should only take the resource's tree-structured layerData into account
         *
         * This means, this serialization is basically only for clients.
         *
         * Sorting an enum by default orders by its ordinal number, not alphanumerically.
         */

        TreeSet<Pair<DeviceLayerKind, String>> sortedLayerStack = new TreeSet<>();
        for (DeviceLayerKind kind : resourceDfn.getLayerStack(accCtx))
        {
            sortedLayerStack.add(new Pair<>(kind, ""));
        }

        sortedLayerStack.addAll(layerStorage.keySet());

        for (Pair<DeviceLayerKind, String> pair : sortedLayerStack)
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
    public String toString()
    {
        return resourceDfn.toString() +
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

    public static enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L),
        ENCRYPTED(2L),
        RESIZE(4L);

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
    }
}
