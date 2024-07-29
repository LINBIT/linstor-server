package com.linbit.linstor.storage.data.adapter.writecache;

import com.linbit.linstor.annotation.Nullable;
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

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WritecacheVlmData<RSC extends AbsResource<RSC>>
    extends AbsVlmData<RSC, WritecacheRscData<RSC>>
    implements WritecacheVlmObject<RSC>, VlmLayerObject<RSC>
{
    // unmodifiable data, once initialized
    private final @Nullable StorPool cacheStorPool;

    // not persisted, serialized, ctrl and stlt
    private @Nullable String diskState;
    private @Nullable String cacheDevice;
    private @Nullable String dataDevice;

    // not persisted, not serialized, stlt only
    private @Nullable String identifier;
    private List<? extends State> unmodStates;
    private @Nullable Size sizeState;

    public WritecacheVlmData(
        AbsVolume<RSC> vlmRef,
        WritecacheRscData<RSC> rscDataRef,
        @Nullable StorPool cacheStorPoolRef,
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

    public void setDataDevice(String dataDeviceRef)
    {
        dataDevice = dataDeviceRef;
    }

    public @Nullable String getCacheDevice()
    {
        return cacheDevice;
    }

    public void setCacheDevice(String cacheDeviceRef)
    {
        cacheDevice = cacheDeviceRef;
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

    public void setDiskState(String diskStateRef)
    {
        diskState = diskStateRef;
    }

    public @Nullable StorPool getCacheStorPool()
    {
        return cacheStorPool;
    }

    @Override
    public WritecacheVlmPojo asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new WritecacheVlmPojo(
            getVlmNr().value,
            devicePath.get(),
            dataDevice,
            cacheDevice,
            cacheStorPool == null ? null : cacheStorPool.getName().displayValue,
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
        return DeviceLayerKind.WRITECACHE;
    }
}
