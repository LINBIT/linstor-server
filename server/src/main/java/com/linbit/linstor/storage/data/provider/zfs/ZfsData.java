package com.linbit.linstor.storage.data.provider.zfs;

import com.linbit.ImplementationError;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsVlmPojo;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsThinVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.ZfsProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import javax.annotation.Nullable;
import javax.inject.Provider;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;

public class ZfsData extends AbsStorageVlmData implements ZfsProviderObject
{
    public static final State CREATED = new State(true, true, "Created");
    public static final State FAILED = new State(false, true, "Failed");

    // not persisted, not serialized, stlt only
    private transient String zpool = null;

    public ZfsData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        DeviceProviderKind kindRef,
        StorPool storPoolRef,
        StorageLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
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
    public String getZPool()
    {
        return zpool;
    }

    @Override
    public void setStorPool(AccessContext accCtxRef, StorPool storPoolRef) throws SQLException, AccessDeniedException
    {
        super.setStorPool(accCtxRef, storPoolRef);
        zpool = null; // force Zfs(Thin)Provider to repeat the lookup using the new storage pool
    }

    public void setZPool(String zpoolRef)
    {
        zpool = zpoolRef;
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
                new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
                storPool.get().getApiData(null, null, accCtxRef, null, null)
            );
        }
        else
        {
            pojo = new ZfsThinVlmPojo(
                getVlmNr().value,
                getDevicePath(),
                getAllocatedSize(),
                getUsableSize(),
                new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
                storPool.get().getApiData(null, null, accCtxRef, null, null)
            );
        }
        return pojo;
    }
}
