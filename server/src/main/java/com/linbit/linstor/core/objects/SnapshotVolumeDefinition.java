package com.linbit.linstor.core.objects;

import com.linbit.drbd.md.MdException;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.pojo.SnapshotVlmDfnPojo;
import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

public class SnapshotVolumeDefinition extends BaseTransactionObject
    implements DbgInstanceUuid, Comparable<SnapshotVolumeDefinition>
{
    public static interface InitMaps
    {
        Map<NodeName, SnapshotVolume> getSnapshotVlmMap();
    }

    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final SnapshotDefinition snapshotDfn;

    // Net volume size in kiB
    private final TransactionSimpleObject<SnapshotVolumeDefinition, Long> volumeSize;

    private final SnapshotVolumeDefinitionDatabaseDriver dbDriver;

    // Properties container for this snapshot volume definition
    private final Props snapshotVlmDfnProps;

    // State flags
    private final StateFlags<Flags> flags;

    private final TransactionMap<NodeName, SnapshotVolume> snapshotVlmMap;

    private final TransactionSimpleObject<SnapshotVolumeDefinition, Boolean> deleted;

    private final VolumeDefinition vlmDfn;
    private final VolumeNumber vlmNr;

    private final TransactionMap<Pair<DeviceLayerKind, String>, VlmDfnLayerObject> layerStorage;


    public SnapshotVolumeDefinition(
        UUID objIdRef,
        SnapshotDefinition snapshotDfnRef,
        VolumeDefinition vlmDfnRef,
        VolumeNumber vlmNrRef,
        long volSize,
        long initFlags,
        SnapshotVolumeDefinitionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<NodeName, SnapshotVolume> snapshotVlmMapRef,
        Map<Pair<DeviceLayerKind, String>, VlmDfnLayerObject> layerDataMapRef
    )
        throws MdException, DatabaseException
    {
        super(transMgrProviderRef);
        VolumeDefinition.checkVolumeSize(volSize);
        vlmDfn = vlmDfnRef;

        objId = objIdRef;
        snapshotDfn = snapshotDfnRef;
        vlmNr = vlmNrRef;
        dbDriver = dbDriverRef;

        dbgInstanceId = UUID.randomUUID();

        snapshotVlmDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                snapshotDfnRef.getResourceName(),
                snapshotDfnRef.getName(),
                vlmNrRef
            )
        );

        flags = transObjFactory.createStateFlagsImpl(
            snapshotDfnRef.getResourceDefinition().getObjProt(),
            this,
            Flags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );

        snapshotVlmMap = transObjFactory.createTransactionMap(snapshotVlmMapRef, null);

        layerStorage = transObjFactory.createTransactionMap(layerDataMapRef, null);

        volumeSize = transObjFactory.createTransactionSimpleObject(
            this,
            volSize,
            dbDriver.getVolumeSizeDriver()
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            layerStorage,
            snapshotDfn,
            snapshotVlmMap,
            deleted
        );
    }

    public UUID getUuid()
    {
        return objId;
    }

    public SnapshotDefinition getSnapshotDefinition()
    {
        checkDeleted();
        return snapshotDfn;
    }

    public VolumeDefinition getVolumeDefinition()
    {
        checkDeleted();
        return vlmDfn;
    }

    public VolumeNumber getVolumeNumber()
    {
        checkDeleted();
        return vlmNr;
    }

    public void addSnapshotVolume(AccessContext accCtx, SnapshotVolume snapshotVolume)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotVlmMap.put(snapshotVolume.getNodeName(), snapshotVolume);
    }

    public void removeSnapshotVolume(
        AccessContext accCtx,
        SnapshotVolume snapshotVolume
    )
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotVlmMap.remove(snapshotVolume.getNodeName());
    }

    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeSize.get();
    }

    public Long setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return volumeSize.set(newVolumeSize);
    }

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, getResourceDefinition().getObjProt(), snapshotVlmDfnProps);
    }

    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @SuppressWarnings("unchecked")
    public <T extends VlmDfnLayerObject> T setLayerData(AccessContext accCtx, T vlmDfnLayerData)
        throws AccessDeniedException
    {
        checkDeleted();
        snapshotDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
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
        snapshotDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

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
        snapshotDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        return (T) layerStorage.get(new Pair<>(kind, rscNameSuffix));
    }

    public void removeLayerData(AccessContext accCtx, DeviceLayerKind kind, String rscNameSuffix)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        snapshotDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        layerStorage.remove(new Pair<>(kind, rscNameSuffix)).delete();
    }

    @Override
    public int compareTo(SnapshotVolumeDefinition otherSnapshotVlmDfn)
    {
        int eq = getSnapshotDefinition().compareTo(
            otherSnapshotVlmDfn.getSnapshotDefinition()
        );
        if (eq == 0)
        {
            eq = getVolumeNumber().compareTo(otherSnapshotVlmDfn.getVolumeNumber());
        }
        return eq;
    }

    public void delete(AccessContext accCtx)
        throws DatabaseException, AccessDeniedException
    {
        if (!deleted.get())
        {
            getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            snapshotDfn.removeSnapshotVolumeDefinition(accCtx, vlmNr);

            snapshotVlmDfnProps.delete();

            for (VlmDfnLayerObject vlmDfnLayerObject : layerStorage.values())
            {
                vlmDfnLayerObject.delete();
            }

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(Boolean.TRUE);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted snapshot volume definition");
        }
    }

    @Override
    public String toString()
    {
        return snapshotDfn + ", VlmNr: '" + vlmDfn.getVolumeNumber() + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public SnapshotVolumeDefinitionApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new SnapshotVlmDfnPojo(
            getUuid(),
            getVolumeNumber().value,
            getVolumeSize(accCtx),
            flags.getFlagsBits(accCtx)
        );
    }

    public ResourceDefinition getResourceDefinition()
    {
        return getSnapshotDefinition().getResourceDefinition();
    }

    public ResourceName getResourceName()
    {
        return getResourceDefinition().getName();
    }

    public SnapshotName getSnapshotName()
    {
        return getSnapshotDefinition().getName();
    }

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        ENCRYPTED(1L);

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

        public static Flags[] restoreFlags(long snapshotVlmDfnFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((snapshotVlmDfnFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new SnapshotVolumeDefinition.Flags[flagList.size()]);
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
}
