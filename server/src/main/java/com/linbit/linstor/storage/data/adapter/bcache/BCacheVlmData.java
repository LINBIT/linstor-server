package com.linbit.linstor.storage.data.adapter.bcache;

import com.linbit.linstor.api.pojo.BCacheRscPojo.BCacheVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.AbsVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.bcache.BCacheVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class BCacheVlmData<RSC extends AbsResource<RSC>>
    extends AbsVlmData<RSC, BCacheRscData<RSC>>
    implements BCacheVlmObject<RSC>, VlmLayerObject<RSC>
{
    // unmodifiable data, once initialized
    private final StorPool cacheStorPool;

    // persisted, serialized, ctrl and stlt
    private final TransactionSimpleObject<BCacheVlmData<?>, UUID> deviceUuid;

    // not persisted, serialized, ctrl and stlt
    private @Nullable String cacheDevice;
    private @Nullable String dataDevice;
    private @Nullable String diskState;

    // not persisted, not serialized, stlt only
    private @Nullable String identifier;
    private List<? extends State> unmodStates;
    private @Nullable Size sizeState;

    public BCacheVlmData(
        AbsVolume<RSC> vlmRef,
        BCacheRscData<RSC> rscDataRef,
        StorPool cacheStorPoolRef,
        LayerBCacheVlmDatabaseDriver bcacheVlmdbDriver,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(vlmRef, rscDataRef, transObjFactory, transMgrProvider);
        cacheStorPool = cacheStorPoolRef; // might be null for peer nodes

        unmodStates = Collections.emptyList();

        deviceUuid = transObjFactory.createTransactionSimpleObject(this, null, bcacheVlmdbDriver.getDeviceUuidDriver());

        transObjs = new ArrayList<>();
        transObjs.add(vlm);
        transObjs.add(rscData);
        transObjs.add(deviceUuid);
        if (cacheStorPool != null)
        {
            transObjs.add(cacheStorPool);
        }
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
    public VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null;
    }

    @Override
    public void setUsableSize(long usableSizeRef) throws DatabaseException
    {
        if (usableSizeRef != usableSize.get())
        {
            if (usableSize.get() < usableSizeRef)
            {
                sizeState = Size.TOO_SMALL;
            }
            else
            {
                sizeState = Size.TOO_LARGE;
            }
        }
        else
        {
            sizeState = Size.AS_EXPECTED;
        }
        usableSize.set(usableSizeRef);
    }

    @Override
    public long getUsableSize()
    {
        return usableSize.get();
    }

    @Override
    public String getDataDevice()
    {
        return dataDevice;
    }

    public void setDataDevice(String dataDeviceRef)
    {
        dataDevice = dataDeviceRef;
    }

    public String getCacheDevice()
    {
        return cacheDevice;
    }

    public void setCacheDevice(String cacheDeviceRef)
    {
        cacheDevice = cacheDeviceRef;
    }

    @Override
    public Size getSizeState()
    {
        return sizeState;
    }

    @Override
    public List<? extends State> getStates()
    {
        return unmodStates;
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

    public String getDiskState()
    {
        return diskState;
    }

    public void setDiskState(String diskStateRef)
    {
        diskState = diskStateRef;
    }

    public StorPool getCacheStorPool()
    {
        return cacheStorPool;
    }

    public UUID getDeviceUuid()
    {
        return deviceUuid.get();
    }

    public void setDeviceUuid(UUID deviceUuidRef) throws DatabaseException
    {
        deviceUuid.set(deviceUuidRef);
    }

    @Override
    public BCacheVlmPojo asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new BCacheVlmPojo(
            getVlmNr().value,
            devicePath.get(),
            dataDevice,
            cacheDevice,
            cacheStorPool == null ? null : cacheStorPool.getName().displayValue,
            allocatedSize.get(),
            usableSize.get(),
            diskState,
            discGran.get(),
            deviceUuid.get(),
            exists.get()
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.BCACHE;
    }
}
