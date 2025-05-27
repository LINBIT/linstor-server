package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerHelperUtils;
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
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import static com.linbit.linstor.storage.kinds.DeviceLayerKind.DRBD;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Set;

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
            null,
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
            snapVlmDfn.getResourceName(),
            snapVlmDfn.getSnapshotName(),
            rscNameSuffix,
            snapVlmDfn.getVolumeNumber(),
            DrbdVlmDfnData.SNAPSHOT_MINOR,
            snapVlmDfn.getSnapshotDefinition().getLayerData(apiCtx, DRBD, rscNameSuffix)
        );
    }

    @Override
    protected DrbdRscData<Snapshot> createSnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Snapshot> parentObjectRef
    )
        throws AccessDeniedException, DatabaseException, ExhaustedPoolException, ValueOutOfRangeException,
        ValueInUseException
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
            drbdRscData.getTcpPortList(),
            drbdRscData.getPortCount(),
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

    @Override
    protected DrbdRscDfnData<Snapshot> restoreSnapDfnData(
        SnapshotDefinition snapDfnRef,
        RscLayerDataApi rscLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    )
        throws DatabaseException, IllegalArgumentException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        DrbdRscPojo drbdRscPojo = (DrbdRscPojo) rscLayerDataApiRef;
        DrbdRscDfnPojo drbdRscDfnPojo = drbdRscPojo.getDrbdRscDfn();

        return layerDataFactory.createDrbdRscDfnData(
            snapDfnRef.getResourceName(),
            snapDfnRef.getName(),
            drbdRscDfnPojo.getRscNameSuffix(),
            drbdRscDfnPojo.getPeerSlots(),
            drbdRscDfnPojo.getAlStripes(),
            drbdRscDfnPojo.getAlStripeSize(),
            null,
            TransportType.valueOfIgnoreCase(drbdRscDfnPojo.getTransportType(), TransportType.IP),
            null // not saving secret
        );
    }

    @Override
    protected DrbdVlmDfnData<Snapshot> restoreSnapVlmDfnData(
        SnapshotVolumeDefinition snapVlmDfn,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    )
        throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        DrbdVlmPojo drbdVlmPojo = (DrbdVlmPojo) vlmLayerDataApiRef;
        DrbdVlmDfnPojo drbdVlmDfnPojo = drbdVlmPojo.getDrbdVlmDfn();
        return layerDataFactory.createDrbdVlmDfnData(
            snapVlmDfn.getVolumeDefinition(),
            snapVlmDfn.getResourceName(),
            snapVlmDfn.getSnapshotName(),
            drbdVlmDfnPojo.getRscNameSuffix(),
            snapVlmDfn.getVolumeNumber(),
            DrbdVlmDfnData.SNAPSHOT_MINOR,
            snapVlmDfn.getSnapshotDefinition().getLayerData(apiCtx, DRBD, drbdVlmDfnPojo.getRscNameSuffix())
        );
    }

    @Override
    protected DrbdRscData<Snapshot> restoreSnapDataImpl(
        Snapshot snapRef,
        RscLayerDataApi rscLayerDataApiRef,
        @Nullable AbsRscLayerObject<Snapshot> parentRef,
        Map<String, String> renameStorPoolMapRef
    )
        throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException, AccessDeniedException, ValueInUseException
    {
        DrbdRscPojo drbdRscPojo = (DrbdRscPojo) rscLayerDataApiRef;
        DrbdRscDfnData<Snapshot> snapDfnData = snapRef.getSnapshotDefinition().getLayerData(
            apiCtx,
            DRBD,
            drbdRscPojo.getRscNameSuffix()
        );

        @Nullable Set<Integer> ports = drbdRscPojo.getPorts();

        return layerDataFactory.createDrbdRscData(
            layerRscIdPool.autoAllocate(),
            snapRef,
            drbdRscPojo.getRscNameSuffix(),
            parentRef,
            snapDfnData,
            new NodeId(drbdRscPojo.getNodeId()),
            ports == null ? null : TcpPortNumber.parse(ports),
            drbdRscPojo.getPortCount(),
            drbdRscPojo.getPeerSlots(),
            drbdRscPojo.getAlStripes(),
            drbdRscPojo.getAlStripeSize(),
            drbdRscPojo.getFlags() // TODO not sure if we should persist this...
        );
    }

    @Override
    protected DrbdVlmData<Snapshot> restoreSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        DrbdRscData<Snapshot> snapDataRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef,
        @Nullable ApiCallRc apiCallRc
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException
    {
        DrbdVlmPojo drbdVlmPojo = (DrbdVlmPojo) vlmLayerDataApiRef;

        String externalMetaDataStorPool = drbdVlmPojo.getExternalMetaDataStorPool();
        StorPool extMdStorPool = null;
        if (externalMetaDataStorPool != null)
        {
            extMdStorPool = AbsLayerHelperUtils.getStorPool(
                apiCtx,
                snapVlmRef,
                snapDataRef,
                externalMetaDataStorPool,
                renameStorPoolMapRef,
                apiCallRc
            );
        }

        return layerDataFactory.createDrbdVlmData(
            snapVlmRef,
            extMdStorPool,
            snapDataRef,
            snapVlmRef.getSnapshotVolumeDefinition().getLayerData(
                apiCtx,
                DRBD,
                snapDataRef.getResourceNameSuffix()
            )
        );
    }
}
