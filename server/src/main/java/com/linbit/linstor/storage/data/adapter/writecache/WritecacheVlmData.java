package com.linbit.linstor.storage.data.adapter.writecache;

import com.linbit.linstor.api.pojo.WritecacheRscPojo.WritecacheVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.AbsVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.writecache.WritecacheVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WritecacheVlmData<RSC extends AbsResource<RSC>>
    extends AbsVlmData<RSC, WritecacheRscData<RSC>>
    implements WritecacheVlmObject<RSC>, VlmLayerObject<RSC>
{
    // unmodifiable data, once initialized
    private final StorPool cacheStorPool;

    // not persisted, serialized, ctrl and stlt
    private @Nullable String devicePathData;
    private @Nullable String devicePathCache;
    private String backingDevice;
    private String diskState;

    // not persisted, not serialized, stlt only
    private String identifier;
    private List<? extends State> unmodStates;
    private Size sizeState;

    public WritecacheVlmData(
        AbsVolume<RSC> vlmRef,
        WritecacheRscData<RSC> rscDataRef,
        StorPool cacheStorPoolRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(vlmRef, rscDataRef, transObjFactory, transMgrProvider);
        cacheStorPool = cacheStorPoolRef; // might be null for peer nodes

        unmodStates = Collections.emptyList();

        transObjs = new ArrayList<>();
        transObjs.add(vlm);
        transObjs.add(rscData);
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

    public String getBackingDevicePath()
    {
        return backingDevice;
    }

    public void setBackingDevice(String backingDeviceRef)
    {
        backingDevice = backingDeviceRef;
    }

    public String getCacheDevicePath()
    {
        return devicePathCache;
    }

    public void setCacheDevice(String devicePathCacheRef)
    {
        devicePathCache = devicePathCacheRef;
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

    @Override
    public WritecacheVlmPojo asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new WritecacheVlmPojo(
            getVlmNr().value,
            devicePathData,
            devicePathCache,
            cacheStorPool == null ? null : cacheStorPool.getName().displayValue,
            allocatedSize.get(),
            usableSize.get(),
            diskState
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.WRITECACHE;
    }
}
