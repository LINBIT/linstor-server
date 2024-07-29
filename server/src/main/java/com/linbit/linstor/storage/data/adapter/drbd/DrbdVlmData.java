package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.AbsVlmData;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DrbdVlmData<RSC extends AbsResource<RSC>>
    extends AbsVlmData<RSC, DrbdRscData<RSC>>
    implements DrbdVlmObject<RSC>
{
    // unmodifiable data, once initialized
    private final DrbdVlmDfnData<RSC> vlmDfnData;

    // persisted, serialized, ctrl and stlt
    private final TransactionSimpleObject<DrbdVlmData<?>, @Nullable StorPool> externalMetaDataStorPool;

    // not persisted, not serialized, stlt only
    private boolean hasMetaData;
    private boolean checkMetaData;
    private boolean isMetaDataNew;
    private boolean hasDisk;
    private final TransactionList<DrbdVlmData<RSC>, State> states;
    private @Nullable Size sizeState;
    private @Nullable String diskState;

    public DrbdVlmData(
        AbsVolume<RSC> vlmRef,
        DrbdRscData<RSC> rscDataRef,
        DrbdVlmDfnData<RSC> vlmDfnDataRef,
        @Nullable StorPool extMetaDataStorPoolRef,
        LayerDrbdVlmDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(vlmRef, rscDataRef, transObjFactoryRef, transMgrProvider);

        vlmDfnData = vlmDfnDataRef;

        checkMetaData = true;
        isMetaDataNew = false;

        externalMetaDataStorPool = transObjFactoryRef.createTransactionSimpleObject(
            this,
            extMetaDataStorPoolRef,
            dbDriverRef.getExtStorPoolDriver()
        );

        states = transObjFactoryRef.createTransactionList(this, new ArrayList<>(), null);

        transObjs = Arrays.asList(
            vlm,
            rscData,
            vlmDfnData,
            states
        );
    }

    @Override
    public long getOriginalSize()
    {
        return originalSize;
    }

    @Override
    public void setOriginalSize(long originalSizeRef)
    {
        originalSize = originalSizeRef;
    }

    @Override
    public @Nullable Size getSizeState()
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
        return DeviceLayerKind.DRBD;
    }

    @Override
    public @Nullable String getMetaDiskPath()
    {
        String metaDiskPath;
        if (getExternalMetaDataStorPool() == null)
        {
            metaDiskPath = null; // internal meta data
        }
        else
        {
            VlmProviderObject<RSC> childVlm = getChildBySuffix(RscLayerSuffixes.SUFFIX_DRBD_META);
            if (childVlm != null)
            {
                // is null if we are nvme-traget while the drbd-ext-metadata stays on the initiator side
                metaDiskPath = childVlm.getDevicePath();
            }
            else
            {
                metaDiskPath = null;
            }
        }
        return metaDiskPath;
    }

    public boolean isUsingExternalMetaData()
    {
        return getExternalMetaDataStorPool() != null;
    }

    @Override
    public @Nullable String getDataDevice()
    {
        VlmProviderObject<RSC> childBySuffix = getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
        String bdDevPath = null;
        if (childBySuffix != null)
        {
            // null when diskless
            bdDevPath = childBySuffix.getDevicePath();
        }
        return bdDevPath;
    }

    @Override
    public @Nullable String getDiskState()
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

    public void setHasDisk(boolean hasDiskRef)
    {
        hasDisk = hasDiskRef;
    }

    public boolean hasMetaData()
    {
        return hasMetaData;
    }

    public void setHasMetaData(boolean hasMetaDataRef)
    {
        hasMetaData = hasMetaDataRef;
    }

    public boolean checkMetaData()
    {
        return checkMetaData;
    }

    public void setCheckMetaData(boolean checkMetaDataRef)
    {
        checkMetaData = checkMetaDataRef;
    }

    public boolean isMetaDataNew()
    {
        return isMetaDataNew;
    }

    public void setMetaDataIsNew(boolean isMetaDataNewRef)
    {
        isMetaDataNew = isMetaDataNewRef;
    }

    public @Nullable StorPool getExternalMetaDataStorPool()
    {
        return externalMetaDataStorPool.get();
    }

    public void setExternalMetaDataStorPool(@Nullable StorPool extMetaStorPool) throws DatabaseException
    {
        externalMetaDataStorPool.set(extMetaStorPool);
    }

    @Override
    public DrbdVlmDfnData<RSC> getVlmDfnLayerObject()
    {
        return vlmDfnData;
    }

    @Override
    public String getIdentifier()
    {
        return rscData.getSuffixedResourceName() + "/" + getVlmNr().value;
    }

    @Override
    public DrbdVlmPojo asPojo(AccessContext accCtxRef)
    {
        String externalMetaDataStorPoolName = null;
        if (getExternalMetaDataStorPool() != null)
        {
            externalMetaDataStorPoolName = getExternalMetaDataStorPool().getName().displayValue;
        }
        return new DrbdVlmPojo(
            vlmDfnData.getApiData(accCtxRef),
            devicePath.get(),
            getDataDevice(),
            externalMetaDataStorPoolName,
            getMetaDiskPath(),
            allocatedSize.get(),
            usableSize.get(),
            diskState,
            discGran.get(),
            exists.get()
        );
    }
}
