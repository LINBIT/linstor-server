package com.linbit.linstor.storage.data.provider.swordfish;

import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishVlmDfnPojo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SwordfishLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.SfVlmDfnProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SfVlmDfnData extends BaseTransactionObject implements SfVlmDfnProviderObject
{
    // unmodifiable data, once initialized
    private final VolumeDefinition vlmDfn;
    private final String rscNameSuffix;
    private final SwordfishLayerDatabaseDriver dbDriver;

    // persisted, serialized
    private final TransactionSimpleObject<SfVlmDfnData, String> vlmOdata;

    // not persisted, not serialized, stlt only
    // TODO: introduce flags instead of failed, sizeStates, states
    private transient boolean exists;
    private final transient List<? extends State> states = new ArrayList<>();
    private transient long size;
    private transient boolean isAttached;

    public SfVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String vlmOdataRef,
        String rscNameSuffixRef,
        SwordfishLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        rscNameSuffix = rscNameSuffixRef;
        dbDriver = dbDriverRef;

        vlmOdata = transObjFactory.createTransactionSimpleObject(this, vlmOdataRef, dbDriverRef.getVlmDfnOdataDriver());
        vlmDfn = Objects.requireNonNull(vlmDfnRef);

        transObjs = Arrays.asList(
            vlmDfn,
            vlmOdata
        );
    }

    @Override
    public String getVlmOdata()
    {
        return vlmOdata.get();
    }

    public void setVlmOdata(String vlmOdataRef) throws DatabaseException
    {
        vlmOdata.set(vlmOdataRef);
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
    public long getSize()
    {
        return size;
    }

    public void setSize(long sizeRef)
    {
        size = sizeRef;
    }

    @Override
    public boolean isAttached()
    {
        return isAttached;
    }

    public void setAttached(boolean isAttachedRef)
    {
        isAttached = isAttachedRef;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public ResourceName getResourceName()
    {
        return vlmDfn.getResourceDefinition().getName();
    }

    @Override
    public SnapshotName getSnapshotName()
    {
        return null;
    }

    @Override
    public VolumeNumber getVolumeNumber()
    {
        return vlmDfn.getVolumeNumber();
    }

    @Override
    public String getRscNameSuffix()
    {
        return rscNameSuffix;
    }

    public String getSuffixedResourceName()
    {
        return vlmDfn.getResourceDefinition().getName().displayValue + rscNameSuffix;
    }

    public VolumeDefinition getVolumeDefinition()
    {
        return vlmDfn;
    }

    @Override
    public void delete() throws DatabaseException
    {
        dbDriver.delete(this);
    }

    @Override
    public SwordfishVlmDfnPojo getApiData(AccessContext accCtxRef)
    {
        return new SwordfishVlmDfnPojo(
            rscNameSuffix,
            vlmDfn.getVolumeNumber().value,
            vlmOdata.get()
        );
    }

}
