package com.linbit.linstor.storage.data.adapter.cache;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.CacheRscPojo.CacheVlmPojo;
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
import com.linbit.linstor.storage.interfaces.layers.cache.CacheVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CacheVlmData<RSC extends AbsResource<RSC>>
    extends AbsVlmData<RSC, CacheRscData<RSC>>
    implements CacheVlmObject<RSC>, VlmLayerObject<RSC>
{
    // unmodifiable data, once initialized
    private final @Nullable StorPool metaStorPool;
    private final @Nullable StorPool cacheStorPool;

    // not persisted, serialized, ctrl and stlt
    private @Nullable String diskState;
    private @Nullable String dataDevice;
    private @Nullable String cacheDevice;
    private @Nullable String metaDevice;

    // not persisted, not serialized, stlt only
    private @Nullable String identifier;
    private List<? extends State> unmodStates;
    private @Nullable Size sizeState;

    public CacheVlmData(
        AbsVolume<RSC> vlmRef,
        CacheRscData<RSC> rscDataRef,
        @Nullable StorPool cacheStorPoolRef,
        @Nullable StorPool metaStorPoolRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(vlmRef, rscDataRef, transObjFactory, transMgrProvider);
        cacheStorPool = cacheStorPoolRef; // might be null for peer nodes
        metaStorPool = metaStorPoolRef; // might be null for peer nodes

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
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
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
    public @Nullable String getDataDevice()
    {
        return dataDevice;
    }

    public void setDataDevice(@Nullable String dataDeviceRef)
    {
        dataDevice = dataDeviceRef;
    }

    public @Nullable String getCacheDevice()
    {
        return cacheDevice;
    }

    public void setCacheDevice(@Nullable String cacheDeviceRef)
    {
        cacheDevice = cacheDeviceRef;
    }

    public @Nullable String getMetaDevice()
    {
        return metaDevice;
    }

    public void setMetaDevice(@Nullable String metaDeviceRef)
    {
        metaDevice = metaDeviceRef;
    }

    @Override
    public @Nullable Size getSizeState()
    {
        return sizeState;
    }

    @Override
    public List<? extends State> getStates()
    {
        return unmodStates;
    }

    @Override
    public @Nullable String getIdentifier()
    {
        return identifier;
    }

    public void setIdentifier(String identifierRef)
    {
        identifier = identifierRef;
    }

    public @Nullable String getDiskState()
    {
        return diskState;
    }

    public void setDiskState(@Nullable String diskStateRef)
    {
        diskState = diskStateRef;
    }

    public @Nullable StorPool getCacheStorPool()
    {
        return cacheStorPool;
    }

    public @Nullable StorPool getMetaStorPool()
    {
        return metaStorPool;
    }

    @Override
    public CacheVlmPojo asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new CacheVlmPojo(
            getVlmNr().value,
            devicePath.get(),
            dataDevice,
            cacheDevice,
            metaDevice,
            cacheStorPool == null ? null : cacheStorPool.getName().displayValue,
            metaStorPool == null ? null : metaStorPool.getName().displayValue,
            allocatedSize.get(),
            usableSize.get(),
            diskState,
            discGran.get(),
            exists.get()
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.CACHE;
    }
}
