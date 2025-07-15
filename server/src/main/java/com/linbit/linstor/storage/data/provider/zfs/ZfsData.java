package com.linbit.linstor.storage.data.provider.zfs;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.ZfsProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.io.File;
import java.util.ArrayList;
import java.util.Objects;

public class ZfsData<RSC extends AbsResource<RSC>>
    extends AbsStorageVlmData<RSC> implements ZfsProviderObject<RSC>
{
    public static final State CREATED = new State(true, true, "Created");
    public static final State FAILED = new State(false, true, "Failed");

    // not persisted, but serialized
    // stlt source, ctrl read only
    private @Nullable Long extentSize = null;

    // not persisted, not serialized, stlt only
    private transient @Nullable String zpool = null;
    private boolean initialShipment;
    private boolean markedForDeletion = false;

    public ZfsData(
        AbsVolume<RSC> vlmRef,
        StorageRscData<RSC> rscDataRef,
        DeviceProviderKind kindRef,
        StorPool storPoolRef,
        LayerStorageVlmDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(
            vlmRef,
            rscDataRef,
            storPoolRef,
            dbDriverRef,
            kindRef,
            transObjFactory,
            transMgrProviderRef
        );
        if (kindRef != DeviceProviderKind.ZFS  && kindRef != DeviceProviderKind.ZFS_THIN)
        {
            throw new ImplementationError("Only ZFS or ZFS_THIN allowed as kinds");
        }
    }

    @Override
    public @Nullable String getZPool()
    {
        return zpool;
    }

    @Override
    public void setStorPool(AccessContext accCtxRef, StorPool storPoolRef)
        throws DatabaseException, AccessDeniedException
    {
        if (!Objects.equals(storPool.get(), storPoolRef))
        {
            super.setStorPool(accCtxRef, storPoolRef);

            // force Zfs(Thin)Provider to repeat the lookup using the new storage pool

            // however, this only needs to be done if the storage pool actually changes. If it is the same SP as before
            // chances are that this ZfsData is part of a DevMgr-external process like cloning. Setting zpool to null in
            // such a scenario might screw up some post-processing like cleanup procedures for the aforementioned
            // DevMgr-external process.
            zpool = null;
        }
    }

    public void setZPool(String zpoolRef)
    {
        zpool = zpoolRef;
    }

    public void setInitialShipment(boolean initialShipmentRef)
    {
        initialShipment = initialShipmentRef;
    }

    public boolean isInitialShipment()
    {
        return initialShipment;
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null; // no special VlmDfnLayerObject for ZFS
    }

    public String getFullQualifiedLvIdentifier()
    {
        return zpool + File.separator + identifier;
    }

    public void setExtentSize(@Nullable Long extentSizeRef)
    {
        // snapshots do not report extentSizes
        extentSize = extentSizeRef;
    }

    public @Nullable Long getExtentSize()
    {
        return extentSize;
    }

    public void setMarkedForDeletion(boolean markedForDeletionRef)
    {
        markedForDeletion = markedForDeletionRef;
    }

    public boolean isMarkedForDeletion()
    {
        return markedForDeletion;
    }
    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        VlmLayerDataApi pojo;
        if (providerKind.equals(DeviceProviderKind.ZFS))
        {
            pojo = new ZfsVlmPojo(
                getVlmNr().value,
                getDevicePath(),
                getAllocatedSize(),
                getUsableSize(),
                getSnapshotAllocatedSize(),
                getSnapshotUsableSize(),
                new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
                discGran.get(),
                storPool.get().getApiData(null, null, accCtxRef, null, null, null, null),
                exists.get(),
                extentSize
            );
        }
        else
        {
            pojo = new ZfsThinVlmPojo(
                getVlmNr().value,
                getDevicePath(),
                getAllocatedSize(),
                getUsableSize(),
                getSnapshotAllocatedSize(),
                getSnapshotUsableSize(),
                new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
                discGran.get(),
                storPool.get().getApiData(null, null, accCtxRef, null, null, null, null),
                exists.get(),
                extentSize
            );
        }
        return pojo;
    }
}
