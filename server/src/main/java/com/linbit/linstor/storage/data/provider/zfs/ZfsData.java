package com.linbit.linstor.storage.data.provider.zfs;

import com.linbit.ImplementationError;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.ZfsThinVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.ZfsProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZfsData extends BaseTransactionObject implements ZfsProviderObject
{
    public static final State CREATED = new State(true, true, "Created");
    public static final State FAILED = new State(false, true, "Failed");

    // unmodifiable data, once initialized
    private final Volume vlm;
    private final DeviceProviderKind providerKind;
    private final StorageRscData rscData;

    // not persisted, serialized
    // TODO: introduce flags instead of exists, failed, sizeStates, states
    private final TransactionList<ZfsData, ? extends State> states;
    private final TransactionSimpleObject<ZfsData, Boolean> exists;
    private final TransactionSimpleObject<ZfsData, Boolean> failed;
    private final TransactionSimpleObject<ZfsData, Long> allocatedSize;
    private final TransactionSimpleObject<ZfsData, Long> usableSize;
    private final TransactionSimpleObject<ZfsData, String> devicePath;
    private final TransactionSimpleObject<ZfsData, Size> sizeState;

    // not persisted, not serialized, stlt only
    private String zpool = null;
    private String identifier = null;


    public ZfsData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        DeviceProviderKind kindRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        rscData = rscDataRef;
        vlm = vlmRef;

        if (kindRef != DeviceProviderKind.ZFS  && kindRef != DeviceProviderKind.ZFS_THIN)
        {
            throw new ImplementationError("Only ZFS or ZFS_THIN allowed as kinds");
        }

        providerKind = kindRef;

        exists = transObjFactory.createTransactionSimpleObject(this, false, null);
        failed = transObjFactory.createTransactionSimpleObject(this, false, null);
        allocatedSize = transObjFactory.createTransactionSimpleObject(this, -1L, null);
        usableSize = transObjFactory.createTransactionSimpleObject(this, -1L, null);
        devicePath = transObjFactory.createTransactionSimpleObject(this, null, null);
        sizeState = transObjFactory.createTransactionSimpleObject(this, null, null);

        states = transObjFactory.createTransactionList(this, new ArrayList<>(), null);

        transObjs = Arrays.asList(
            vlm,
            rscData,
            exists,
            failed,
            allocatedSize,
            usableSize,
            devicePath,
            sizeState
        );
    }

    @Override
    public boolean exists()
    {
        return exists.get();
    }

    public void setExists(boolean existsRef) throws SQLException
    {
        exists.set(existsRef);
    }

    @Override
    public boolean isFailed()
    {
        return failed.get();
    }

    public void setFailed(boolean failedRef) throws SQLException
    {
        failed.set(failedRef);
    }

    @Override
    public String getZPool()
    {
        return zpool;
    }

    public void setZPool(String zpoolRef)
    {
        zpool = zpoolRef;
    }

    @Override
    public String getIdentifier()
    {
        return identifier;
    }

    public void setIdentifier(String identifierRef)
    {
        identifier = identifierRef;
    }

    @Override
    public List<? extends State> getStates()
    {
        return states;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public DeviceProviderKind getProviderKind()
    {
        return providerKind;
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize.get();
    }

    public void setAllocatedSize(long allocatedSizeRef) throws SQLException
    {
        allocatedSize.set(allocatedSizeRef);
    }

    @Override
    public long getUsableSize()
    {
        return usableSize.get();
    }

    public void setUsableSize(long usableSizeRef) throws SQLException
    {
        usableSize.set(usableSizeRef);
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null;
    }

    @Override
    public RscLayerObject getRscLayerObject()
    {
        return rscData;
    }

    @Override
    public String getDevicePath()
    {
        return devicePath.get();
    }

    public void setDevicePath(String devicePathRef) throws SQLException
    {
        devicePath.set(devicePathRef);
    }

    @Override
    public Size getSizeState()
    {
        return sizeState.get();
    }

    public void setSizeState(Size sizeStateRef) throws SQLException
    {
        sizeState.set(sizeStateRef);
    }

    public String getFullQualifiedLvIdentifier()
    {
        return zpool + File.separator + identifier;
    }

    @Override
    public VlmLayerDataPojo asPojo(AccessContext accCtxRef)
    {
        VlmLayerDataPojo pojo;
        if (providerKind.equals(DeviceProviderKind.ZFS))
        {
            pojo = new ZfsVlmPojo(
                getVlmNr().value,
                getDevicePath(),
                getAllocatedSize(),
                getUsableSize()
            );
        }
        else
        {
            pojo = new ZfsThinVlmPojo(
                getVlmNr().value,
                getDevicePath(),
                getAllocatedSize(),
                getUsableSize()
            );
        }
        return pojo;
    }
}
