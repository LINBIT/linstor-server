package com.linbit.linstor.storage.data.adapter.nvme;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.NvmeRscPojo.NvmeVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.AbsVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.nvme.NvmeVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NvmeVlmData<RSC extends AbsResource<RSC>>
    extends AbsVlmData<RSC, NvmeRscData<RSC>>
    implements NvmeVlmObject<RSC>, VlmLayerObject<RSC>
{
    // not persisted, not serialized, stlt only
    private final TransactionList<NvmeVlmData<RSC>, State> states;
    private @Nullable Size sizeState;
    private @Nullable String diskState;

    public NvmeVlmData(
        AbsVolume<RSC> vlmRef,
        NvmeRscData<RSC> rscDataRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(vlmRef, rscDataRef, transObjFactoryRef, transMgrProvider);
        states = transObjFactoryRef.createTransactionList(this, new ArrayList<>(), null);
        transObjs = Arrays.asList(
            vlm,
            rscData,
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
    public long getUsableSize()
    {
        return usableSize.get();
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
        return DeviceLayerKind.NVME;
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

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null;
    }

    @Override
    public String getIdentifier()
    {
        return rscData.getSuffixedResourceName() + "/" + getVlmNr().getValue();
    }

    @Override
    public NvmeVlmPojo asPojo(AccessContext accCtxRef)
    {
        return new NvmeVlmPojo(
            getVlmNr().getValue(),
            devicePath.get(),
            getDataDevice(),
            allocatedSize.get(),
            usableSize.get(),
            diskState,
            discGran.get(),
            exists.get()
        );
    }
}
