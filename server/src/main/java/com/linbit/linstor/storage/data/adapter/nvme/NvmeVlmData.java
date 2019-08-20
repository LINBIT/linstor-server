package com.linbit.linstor.storage.data.adapter.nvme;

import com.linbit.linstor.api.pojo.NvmeRscPojo.NvmeVlmPojo;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.nvme.NvmeVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.annotation.Nullable;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NvmeVlmData extends BaseTransactionObject implements NvmeVlmObject
{
    // unmodifiable data, once initialized
    private final Volume vlm;
    private final NvmeRscData rscData;

    // not persisted, serialized, ctrl and stlt
    private long allocatedSize;
    private String devicePath;
    private long usableSize;

    // not persisted, not serialized, stlt only
    private boolean exists;
    private boolean failed;
    private boolean hasDisk;
    private final TransactionList<NvmeVlmData, State> states;
    private Size sizeState;
    private String diskState;

    public NvmeVlmData(
        Volume vlmRef,
        NvmeRscData rscDataRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        vlm = vlmRef;
        rscData = rscDataRef;
        exists = false;
        failed = false;
        states = transObjFactoryRef.createTransactionList(this, new ArrayList<>(), null);
        transObjs = Arrays.asList(
            vlm,
            rscData,
            states
        );
    }

    @Override
    public boolean exists()
    {
        return exists;
    }

    public void setExists(boolean existsRef)
    {
        exists = existsRef;
    }

    @Override
    public boolean hasFailed()
    {
        return failed;
    }

    public void setFailed(boolean failedRef)
    {
        failed = failedRef;
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize;
    }

    public void setAllocatedSize(long allocatedSizeRef)
    {
        allocatedSize = allocatedSizeRef;
    }

    @Override
    public long getUsableSize()
    {
        return usableSize;
    }

    @Override
    public void setUsableSize(long usableSizeRef)
    {
        usableSize = usableSizeRef;
    }

    @Override
    public String getDevicePath()
    {
        return devicePath;
    }

    public void setDevicePath(String devicePathRef)
    {
        devicePath = devicePathRef;
    }

    @Override
    public Size getSizeState()
    {
        return sizeState;
    }

    public void setSizeState(Size sizeStateRef)
    {
        sizeState = sizeStateRef;
    }

    @Override
    public List<? extends State> getStates()
    {
        return states;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.NVME;
    }

    @Override
    public String getDiskState()
    {
        return diskState;
    }

    public void setDiskState(String diskStateRef)
    {
        diskState = diskStateRef;
    }

    public boolean hasDisk()
    {
        return hasDisk;
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
    public NvmeRscData getRscLayerObject()
    {
        return rscData;
    }

    @Override
    public String getIdentifier()
    {
        return rscData.getSuffixedResourceName() + "/" + getVlmNr().getValue();
    }

    @Override
    public NvmeVlmPojo asPojo(AccessContext accCtxRef)
    {
        return new NvmeVlmPojo(
            getVlmNr().getValue(),
            devicePath,
            getBackingDevice(),
            allocatedSize,
            usableSize,
            diskState
        );
    }
}
