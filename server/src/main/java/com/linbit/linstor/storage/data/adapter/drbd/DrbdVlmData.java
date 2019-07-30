package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DrbdVlmData extends BaseTransactionObject implements DrbdVlmObject
{
    // unmodifiable data, once initialized
    private final Volume vlm;
    private final DrbdRscData rscData;
    private final DrbdVlmDfnData vlmDfnData;

    // persisted, serialized, ctrl and stlt
    private final TransactionSimpleObject<DrbdVlmData, StorPool> externalMetaDataStorPool;

    // not persisted, serialized, ctrl and stlt
    private long allocatedSize;
    private String devicePath;
    private long usableSize;

    // not persisted, not serialized, stlt only
    private boolean exists;
    private boolean failed;
    private boolean hasMetaData;
    private boolean checkMetaData;
    private boolean isMetaDataNew;
    private boolean hasDisk;
    private final TransactionList<DrbdVlmData, State> states;
    private Size sizeState;
    private String diskState;

    public DrbdVlmData(
        Volume vlmRef,
        DrbdRscData rscDataRef,
        DrbdVlmDfnData vlmDfnDataRef,
        StorPool extMetaDataStorPoolRef,
        DrbdLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);

        vlm = vlmRef;
        rscData = rscDataRef;
        vlmDfnData = vlmDfnDataRef;

        exists = false;
        failed = false;

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
        return DeviceLayerKind.DRBD;
    }

    @Override
    public String getMetaDiskPath()
    {
        String metaDiskPath;
        if (getExternalMetaDataStorPool() == null)
        {
            metaDiskPath = null; // internal meta data
        }
        else
        {
            metaDiskPath = getChildBySuffix(DrbdRscData.SUFFIX_META).getDevicePath();
        }
        return metaDiskPath;
    }

    public boolean isUsingExternalMetaData()
    {
        return getExternalMetaDataStorPool() != null;
    }

    @Override
    public String getBackingDevice()
    {
        return getChildBySuffix(DrbdRscData.SUFFIX_DATA).getDevicePath();
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

    public StorPool getExternalMetaDataStorPool()
    {
        return externalMetaDataStorPool.get();
    }

    public void setExternalMetaDataStorPool(StorPool extMetaStorPool) throws DatabaseException
    {
        externalMetaDataStorPool.set(extMetaStorPool);
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }

    @Override
    public DrbdVlmDfnData getVlmDfnLayerObject()
    {
        return vlmDfnData;
    }

    @Override
    public DrbdRscData getRscLayerObject()
    {
        return rscData;
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
            devicePath,
            getBackingDevice(),
            externalMetaDataStorPoolName,
            getMetaDiskPath(),
            allocatedSize,
            usableSize,
            diskState
        );
    }
}
