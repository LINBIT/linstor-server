package com.linbit.linstor.storage.data.adapter.nvme;

import com.linbit.linstor.api.pojo.OpenflexRscPojo.OpenflexVlmPojo;
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
import com.linbit.linstor.storage.interfaces.layers.nvme.OpenflexVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OpenflexVlmData<RSC extends AbsResource<RSC>>
    extends AbsVlmData<RSC, OpenflexRscData<RSC>>
    implements OpenflexVlmObject<RSC>, VlmLayerObject<RSC>
{
    // unmodifiable data, once initialized
    private final StorPool storPool;

    // not persisted, not serialized, stlt only
    private final TransactionList<OpenflexVlmData<RSC>, State> states;
    private Size sizeState;
    private String diskState;
    private String ofId;
    protected transient long expectedSize;

    public OpenflexVlmData(
        AbsVolume<RSC> vlmRef,
        OpenflexRscData<RSC> rscDataRef,
        StorPool storPoolRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(vlmRef, rscDataRef, transObjFactoryRef, transMgrProviderRef);
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
            devicePath.get(),
            ofId,
            allocatedSize.get(),
            usableSize.get(),
            diskState,
            storPool.getApiData(null, null, accCtxRef, null, null),
            discGran.get()
        );
    }
}
