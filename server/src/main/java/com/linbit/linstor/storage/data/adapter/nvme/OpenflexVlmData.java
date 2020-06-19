package com.linbit.linstor.storage.data.adapter.nvme;

import com.linbit.linstor.api.pojo.OpenflexRscPojo.OpenflexVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.nvme.OpenflexVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OpenflexVlmData<RSC extends AbsResource<RSC>>
    extends BaseTransactionObject
    implements OpenflexVlmObject<RSC>
{
    // unmodifiable data, once initialized
    private final AbsVolume<RSC> vlm;
    private final OpenflexRscData<RSC> rscData;
    private final StorPool storPool;

    // not persisted, serialized, ctrl and stlt
    private long allocatedSize = UNINITIALIZED_SIZE;
    private String devicePath;
    private long usableSize = UNINITIALIZED_SIZE;

    // not persisted, not serialized, stlt only
    private boolean exists;
    private boolean failed;
    private final TransactionList<OpenflexVlmData<RSC>, State> states;
    private Size sizeState;
    private String diskState;
    private String ofId;
    protected transient long expectedSize;
    private long originalSize = UNINITIALIZED_SIZE;

    public OpenflexVlmData(
        AbsVolume<RSC> vlmRef,
        OpenflexRscData<RSC> rscDataRef,
        StorPool storPoolRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        vlm = vlmRef;
        rscData = rscDataRef;
        storPool = storPoolRef;

        states = transObjFactoryRef.createTransactionList(this, new ArrayList<>(), null);

        transObjs = Arrays.asList(
            vlm,
            rscData,
            states
        );
    }

    @Override
    public StorPool getStorPool()
    {
        return storPool;
    }

    public void setExists(boolean existsRef)
    {
        exists = existsRef;
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
    public long getAllocatedSize()
    {
        return allocatedSize;
    }

    @Override
    public void setAllocatedSize(long allocatedSizeRef)
    {
        allocatedSize = allocatedSizeRef;
    }

    @Override
    public AbsVolume<RSC> getVolume()
    {
        return vlm;
    }

    @Override
    public VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null;
    }

    @Override
    public OpenflexRscData<RSC> getRscLayerObject()
    {
        return rscData;
    }

    @Override
    public void setUsableSize(long usableSizeRef)
    {
        if (usableSizeRef != usableSize)
        {
            if (usableSize < usableSizeRef)
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
        usableSize = usableSizeRef;
    }

    @Override
    public long getUsableSize()
    {
        return usableSize;
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

    @Override
    public List<? extends State> getStates()
    {
        return states;
    }

    /**
     * Returns the identifier from the openflex API, NOT the "rscName_vlmNr" format
     */
    @Override
    public String getIdentifier()
    {
        return ofId;
    }

    public void setIdentifier(String ofIdRef)
    {
        ofId = ofIdRef;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.OPENFLEX;
    }

    public void setExepectedSize(long size)
    {
        expectedSize = size;
    }

    public long getExepectedSize()
    {
        return expectedSize;
    }

    public String getDiskState()
    {
        return diskState;
    }

    public void setDiskState(String diskStateRef)
    {
        diskState = diskStateRef;
    }

    @Override
    public OpenflexVlmPojo asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new OpenflexVlmPojo(
            getVlmNr().value,
            devicePath,
            ofId,
            allocatedSize,
            usableSize,
            diskState,
            storPool.getApiData(null, null, accCtxRef, null, null)
        );
    }
}
