package com.linbit.linstor.core.objects;

import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.SnapshotVlmDfnPojo;
import com.linbit.linstor.api.prop.LinStorObject;
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
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
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
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;

public class SnapshotVolumeDefinition extends AbsCoreObj<SnapshotVolumeDefinition> implements ProtectedObject
{
    public interface InitMaps
    {
        Map<NodeName, SnapshotVolume> getSnapshotVlmMap();
    }

    private final SnapshotDefinition snapshotDfn;

    // Net volume size in kiB
    private final TransactionSimpleObject<SnapshotVolumeDefinition, Long> volumeSize;

    private final SnapshotVolumeDefinitionDatabaseDriver dbDriver;

    // Properties container for this snapshot volume definition
    private final Props snapVlmDfnProps;
    // Properties container for this snapshot volume definition
    private final Props vlmDfnProps;
    private final ReadOnlyProps vlmDfnRoProps;

    // State flags
    private final StateFlags<Flags> flags;

    private final TransactionMap<SnapshotVolumeDefinition, NodeName, SnapshotVolume> snapshotVlmMap;

    private final VolumeDefinition vlmDfn;
    private final VolumeNumber vlmNr;

    private final TransactionMap<SnapshotVolumeDefinition, Pair<DeviceLayerKind, String>, VlmDfnLayerObject> layerStorage;

    private final Key snapVlmDfnKey;

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
        super(objIdRef, transObjFactory, transMgrProviderRef);
        VolumeDefinition.checkVolumeSize(volSize, layerDataMapRef);
        vlmDfn = vlmDfnRef;

        snapshotDfn = snapshotDfnRef;
        vlmNr = vlmNrRef;
        dbDriver = dbDriverRef;
        snapVlmDfnKey = new Key(this);

        snapVlmDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                snapshotDfnRef.getResourceName(),
                snapshotDfnRef.getName(),
                vlmNrRef,
                false
            ),
            toStringImpl(),
            LinStorObject.SNAP_VLM_DFN
        );
        vlmDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                snapshotDfnRef.getResourceName(),
                snapshotDfnRef.getName(),
                vlmNrRef,
                true
            ),
            toStringImpl(),
            LinStorObject.VLM_DFN
        );
        vlmDfnRoProps = new ReadOnlyPropsImpl(vlmDfnProps);

        flags = transObjFactory.createStateFlagsImpl(
            snapshotDfnRef.getResourceDefinition().getObjProt(),
            this,
            Flags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );

        snapshotVlmMap = transObjFactory.createTransactionMap(this, snapshotVlmMapRef, null);

        layerStorage = transObjFactory.createTransactionMap(this, layerDataMapRef, null);

        volumeSize = transObjFactory.createTransactionSimpleObject(
            this,
            volSize,
            dbDriver.getVolumeSizeDriver()
        );

        transObjs = Arrays.asList(
            layerStorage,
            snapshotDfn,
            snapshotVlmMap,
            deleted,
            snapVlmDfnProps,
            vlmDfnProps
        );
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

    public Key getKey()
    {
        return snapVlmDfnKey;
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

    public Props getSnapVlmDfnProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, getResourceDefinition().getObjProt(), snapVlmDfnProps);
    }

    public ReadOnlyProps getVlmDfnProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return vlmDfnRoProps;
    }

    public Props getVlmDfnPropsForChange(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return vlmDfnProps;
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
    public <T extends VlmDfnLayerObject> @Nullable T getLayerData(
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

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(snapshotDfn, vlmNr);
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
        else if (obj instanceof SnapshotVolumeDefinition)
        {
            SnapshotVolumeDefinition other = (SnapshotVolumeDefinition) obj;
            other.checkDeleted();
            ret = Objects.equals(snapshotDfn, other.snapshotDfn) && Objects.equals(vlmNr, other.vlmNr);
        }
        return ret;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws DatabaseException, AccessDeniedException
    {
        if (!deleted.get())
        {
            getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            snapshotDfn.removeSnapshotVolumeDefinition(accCtx, vlmNr);

            snapVlmDfnProps.delete();
            vlmDfnProps.delete();

            for (VlmDfnLayerObject vlmDfnLayerObject : layerStorage.values())
            {
                vlmDfnLayerObject.delete();
            }

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(Boolean.TRUE);
        }
    }

    @Override
    public String toStringImpl()
    {
        return "RscName: " + snapVlmDfnKey.rscName + ", SnapName: " + snapVlmDfnKey.snapName +
            ", VlmNr: '" + vlmNr + "'";
    }

    public SnapshotVolumeDefinitionApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new SnapshotVlmDfnPojo(
            getUuid(),
            getVolumeNumber().value,
            getVolumeSize(accCtx),
            flags.getFlagsBits(accCtx),
            snapVlmDfnProps.map(),
            vlmDfnProps.map()
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
        ENCRYPTED(1L),
        GROSS_SIZE(1L << 1);

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

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return getResourceDefinition().getObjProt();
    }

    /**
     * Identifies a nodeConnection.
     */
    public static class Key implements Comparable<Key>
    {
        private final ResourceName rscName;
        private final SnapshotName snapName;
        private final VolumeNumber vlmNr;

        public Key(SnapshotVolumeDefinition snapVlmDfn)
        {
            this(snapVlmDfn.getResourceName(), snapVlmDfn.getSnapshotName(), snapVlmDfn.getVolumeNumber());
        }

        public Key(ResourceName rscNameRef, SnapshotName snapNameRef, VolumeNumber vlmNrRef)
        {
            rscName = rscNameRef;
            snapName = snapNameRef;
            vlmNr = vlmNrRef;
        }

        public ResourceName getRscName()
        {
            return rscName;
        }

        public SnapshotName getSnapName()
        {
            return snapName;
        }

        public VolumeNumber getVlmNr()
        {
            return vlmNr;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rscName, snapName, vlmNr);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof Key))
            {
                return false;
            }
            Key other = (Key) obj;
            return Objects.equals(rscName, other.rscName) && Objects.equals(snapName, other.snapName) && Objects.equals(
                vlmNr,
                other.vlmNr
            );
        }

        @Override
        public int compareTo(Key other)
        {
            int eq = rscName.compareTo(other.rscName);
            if (eq == 0)
            {
                eq = snapName.compareTo(other.snapName);
                if (eq == 0)
                {
                    eq = vlmNr.compareTo(other.vlmNr);
                }
            }
            return eq;
        }
    }
}
