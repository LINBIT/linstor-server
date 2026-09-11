package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.LuksRscPojo.LuksVlmPojo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.util.Map;

@Singleton
class SnapLuksLayerHelper extends AbsSnapLayerHelper<
    LuksRscData<Snapshot>, LuksVlmData<Snapshot>,
    RscDfnLayerObject, VlmDfnLayerObject
>
{
    @Inject
    SnapLuksLayerHelper(
        ErrorReporter errorReporterRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef
    )
    {
        super(
            errorReporterRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            DeviceLayerKind.LUKS
        );
    }

    @Override
    protected @Nullable RscDfnLayerObject createSnapDfnData(SnapshotDefinition rscDfnRef, String rscNameSuffixRef)
        throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // LuksLayer does not have resource-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected @Nullable VlmDfnLayerObject createSnapVlmDfnData(
        SnapshotVolumeDefinition snapVlmDfnRef,
        String rscNameSuffixRef
    )
        throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
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
        throws DatabaseException, ExhaustedPoolException
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
            throws DatabaseException
    {
        LuksVlmData<Resource> luksVlmData = (LuksVlmData<Resource>) vlmDataRef;
        return layerDataFactory.createLuksVlmData(
            snapVlmRef,
            snapDataRef,
            luksVlmData.getEncryptedKey()
        );
    }

    @Override
    protected @Nullable RscDfnLayerObject restoreSnapDfnData(
        SnapshotDefinition snapshotDefinitionRef,
        RscLayerDataApi rscLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, IllegalArgumentException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // LuksLayer does not have resource-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected @Nullable VlmDfnLayerObject restoreSnapVlmDfnData(
        SnapshotVolumeDefinition snapshotVolumeDefinitionRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // LuksLayer does not have volume-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected LuksRscData<Snapshot> restoreSnapDataImpl(
        Snapshot snapRef,
        RscLayerDataApi rscLayerDataApiRef,
        @Nullable AbsRscLayerObject<Snapshot> parentRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException
    {
        return layerDataFactory.createLuksRscData(
            layerRscIdPool.autoAllocate(),
            snapRef,
            rscLayerDataApiRef.getRscNameSuffix(),
            parentRef
        );
    }

    @Override
    protected LuksVlmData<Snapshot> restoreSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        LuksRscData<Snapshot> snapDataRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef,
        @Nullable ApiCallRc apiCallRc
    ) throws InvalidNameException, DatabaseException
    {
        LuksVlmPojo luksVlmPojo = (LuksVlmPojo) vlmLayerDataApiRef;
        return layerDataFactory.createLuksVlmData(
            snapVlmRef,
            snapDataRef,
            luksVlmPojo.getEncryptedPassword()
        );
    }
}
