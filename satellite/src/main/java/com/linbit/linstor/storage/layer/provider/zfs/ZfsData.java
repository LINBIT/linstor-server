package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.ImplementationError;
import com.linbit.linstor.Volume;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.ZfsProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.provider.StorageRscData;
import com.linbit.linstor.storage.utils.ZfsUtils.ZfsInfo;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZfsData extends BaseTransactionObject implements ZfsProviderObject
{
    public static final State CREATED = new State(true, true, "Created");
    public static final State FAILED = new State(false, true, "Failed");
    private final DeviceProviderKind providerKind;

    boolean exists = false;
    boolean failed = false;
    String zpool = null;
    String identifier = null;
    Size sizeState = null;

    List<State> states = new ArrayList<>();
    String devicePath;
    StorageRscData rscData;
    Volume vlm;
    long usableSize;
    long allocatedSize;

    public ZfsData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        DeviceProviderKind kindRef,
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

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
        );
    }

    void updateInfo(ZfsInfo info)
    {
        exists = true;
        zpool = info.poolName;
        identifier = info.identifier;
        allocatedSize = info.size;
        usableSize = info.size;
        devicePath = info.path;
    }

    @Override
    public boolean exists()
    {
        return exists;
    }

    @Override
    public boolean isFailed()
    {
        return failed;
    }

    @Override
    public String getZPool()
    {
        return zpool;
    }

    @Override
    public String getIdentifier()
    {
        return identifier;
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
        return allocatedSize;
    }

    @Override
    public long getUsableSize()
    {
        return usableSize;
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
        return devicePath;
    }

    @Override
    public Size getSizeState()
    {
        return sizeState;
    }
}
