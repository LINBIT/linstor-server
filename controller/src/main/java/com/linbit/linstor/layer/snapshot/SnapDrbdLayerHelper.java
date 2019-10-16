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
// import com.linbit.linstor.layer.LayerPayload;
// import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import static com.linbit.linstor.storage.kinds.DeviceLayerKind.DRBD;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
class SnapDrbdLayerHelper extends AbsSnapLayerHelper<
    DrbdRscData<Snapshot>, DrbdVlmData<Snapshot>,
    DrbdRscDfnData<Snapshot>, DrbdVlmDfnData<Snapshot>
>
{
    @Inject
    SnapDrbdLayerHelper(
        ErrorReporter errorReporter,
        @ApiContext AccessContext apiCtx,
        LayerDataFactory layerDataFactory,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPool
    )
    {
        super(
            errorReporter,
            apiCtx,
            layerDataFactory,
            layerRscIdPool,
            DRBD
        );
    }

    @Override
    protected DrbdRscDfnData<Snapshot> createSnapDfnData(
        SnapshotDefinition snapDfn,
        String rscNameSuffix
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        DrbdRscDfnData<Resource> rscDfnData = snapDfn.getResourceDefinition().getLayerData(
            apiCtx,
            DRBD,
            rscNameSuffix
        );

        return layerDataFactory.createDrbdRscDfnData(
            rscDfnData.getResourceName(),
            snapDfn.getName(),
            rscNameSuffix,
            rscDfnData.getPeerSlots(),
            rscDfnData.getAlStripes(),
            rscDfnData.getAlStripeSize(),
            DrbdRscDfnData.SNAPSHOT_TCP_PORT,
            rscDfnData.getTransportType(),
            null // not saving secret
        );
    }

    @Override
    protected DrbdVlmDfnData<Snapshot> createSnapVlmDfnData(
        SnapshotVolumeDefinition snapVlmDfn,
        String rscNameSuffix
    )
        throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        return layerDataFactory.createDrbdVlmDfnData(
            snapVlmDfn.getVolumeDefinition(),
            snapVlmDfn.getSnapshotName(),
            rscNameSuffix,
            null, // not saving minor nr
            snapVlmDfn.getSnapshotDefinition().getLayerData(apiCtx, DRBD, rscNameSuffix)
        );
    }

    @Override
    protected DrbdRscData<Snapshot> createSnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Snapshot> parentObjectRef
    )
        throws AccessDeniedException, DatabaseException, ExhaustedPoolException
    {
        DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) rscDataRef;
        DrbdRscDfnData<Snapshot> snapDfnData = snapRef.getSnapshotDefinition().getLayerData(
            apiCtx,
            DRBD,
            drbdRscData.getResourceNameSuffix()
        );

        return layerDataFactory.createDrbdRscData(
            layerRscIdPool.autoAllocate(),
            snapRef,
            rscDataRef.getResourceNameSuffix(),
            parentObjectRef,
            snapDfnData,
            drbdRscData.getNodeId(),
            drbdRscData.getPeerSlots(),
            drbdRscData.getAlStripes(),
            drbdRscData.getAlStripeSize(),
            drbdRscData.getFlags().getFlagsBits(apiCtx) // TODO not sure if we should persist this...
        );
    }

    @Override
    protected DrbdVlmData<Snapshot> createSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        DrbdRscData<Snapshot> drbdSnapDataRef,
        VlmProviderObject<Resource> vlmDataRef
    )
        throws DatabaseException, AccessDeniedException
    {
        DrbdVlmData<Resource> drbdVlmData = (DrbdVlmData<Resource>) vlmDataRef;

        return layerDataFactory.createDrbdVlmData(
            snapVlmRef,
            drbdVlmData.getExternalMetaDataStorPool(),
            drbdSnapDataRef,
            snapVlmRef.getSnapshotVolumeDefinition().getLayerData(apiCtx, DRBD, drbdSnapDataRef.getResourceNameSuffix())
        );
    }
}
