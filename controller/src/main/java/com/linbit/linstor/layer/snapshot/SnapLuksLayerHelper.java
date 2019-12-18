package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
class SnapLuksLayerHelper extends AbsSnapLayerHelper<
    LuksRscData<Snapshot>, LuksVlmData<Snapshot>,
    RscDfnLayerObject, VlmDfnLayerObject
>
{
    @Inject
    SnapLuksLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            DeviceLayerKind.LUKS
        );
    }

    @Override
    protected RscDfnLayerObject createSnapDfnData(SnapshotDefinition rscDfnRef, String rscNameSuffixRef)
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // LuksLayer does not have resource-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected VlmDfnLayerObject createSnapVlmDfnData(
        SnapshotVolumeDefinition snapVlmDfnRef,
        String rscNameSuffixRef
    )
        throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // LuksLayer does not have volume-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected LuksRscData<Snapshot> createSnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Snapshot> parentObjectRef
    )
        throws AccessDeniedException, DatabaseException, ExhaustedPoolException
    {
        return layerDataFactory.createLuksRscData(
            layerRscIdPool.autoAllocate(),
            snapRef,
            rscDataRef.getResourceNameSuffix(),
            parentObjectRef
        );
    }

    @Override
    protected LuksVlmData<Snapshot> createSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        LuksRscData<Snapshot> snapDataRef,
        VlmProviderObject<Resource> vlmDataRef
    )
            throws DatabaseException, AccessDeniedException
    {
        LuksVlmData<Resource> luksVlmData = (LuksVlmData<Resource>) vlmDataRef;
        return layerDataFactory.createLuksVlmData(
            snapVlmRef,
            snapDataRef,
            luksVlmData.getEncryptedKey()
        );
    }
}
