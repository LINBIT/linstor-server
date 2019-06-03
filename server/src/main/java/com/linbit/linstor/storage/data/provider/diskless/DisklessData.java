package com.linbit.linstor.storage.data.provider.diskless;

import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.DisklessVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DisklessData extends BaseTransactionObject implements VlmProviderObject
{
    private final Volume vlm;
    private final RscLayerObject rscData;
    private final List<? extends State> unmodStates;

    private final TransactionSimpleObject<DisklessData, Long> usableSize;

    private final boolean exists = true;
    private final boolean failed = false;
    private final List<? extends State> states;

    public DisklessData(
        Volume vlmRef,
        RscLayerObject rscDataRef,
        long usableSizeRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        rscData = rscDataRef;
        vlm = Objects.requireNonNull(vlmRef);

        states = new ArrayList<>();
        unmodStates = Collections.unmodifiableList(states);

        usableSize = transObjFactory.createTransactionSimpleObject(this, usableSizeRef, null);

        transObjs = Arrays.asList(
            vlm,
            rscData,
            usableSize
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public DeviceProviderKind getProviderKind()
    {
        return DeviceProviderKind.DISKLESS;
    }

    @Override
    public boolean exists()
    {
        return exists;
    }

    @Override
    public boolean hasFailed()
    {
        return failed;
    }

    @Override
    public long getAllocatedSize()
    {
        return 0;
    }

    @Override
    public long getUsableSize()
    {
        return usableSize.get();
    }

    @Override
    public void setUsableSize(long usableSizeRef) throws SQLException
    {
        usableSize.set(usableSizeRef);
    }

    @Override
    public String getDevicePath()
    {
        return null;
    }

    @Override
    public Size getSizeState()
    {
        return Size.AS_EXPECTED;
    }

    @Override
    public List<? extends State> getStates()
    {
        return unmodStates;
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }

    @Override
    public VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null;
    }

    @Override
    public RscLayerObject getRscLayerObject()
    {
        return rscData;
    }

    @Override
    public String getIdentifier()
    {
        return rscData.getSuffixedResourceName() + "/" + getVlmNr().value;
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef)
    {
        return new DisklessVlmPojo(
            vlm.getVolumeDefinition().getVolumeNumber().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            null
        );
    }
}
