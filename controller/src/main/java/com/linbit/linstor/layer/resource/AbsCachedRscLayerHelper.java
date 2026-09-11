package com.linbit.linstor.layer.resource;

import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import jakarta.inject.Provider;

import java.util.List;

public abstract class AbsCachedRscLayerHelper<
    RSC_LO extends AbsRscLayerObject<Resource>,
    VLM_LO extends VlmProviderObject<Resource>,
    RSC_DFN_LO extends RscDfnLayerObject,
    VLM_DFN_LO extends VlmDfnLayerObject>
    extends AbsRscLayerHelper<RSC_LO, VLM_LO, RSC_DFN_LO, VLM_DFN_LO>
{

    protected AbsCachedRscLayerHelper(
        ErrorReporter errorReporterRef,
        LayerDataFactory layerDataFactoryRef,
        DynamicNumberPool layerRscIdPoolRef,
        Class<RSC_LO> rscClassRef,
        DeviceLayerKind kindRef,
        Provider<CtrlRscLayerDataFactory> layerDataHelperProviderRef
    )
    {
        super(
            errorReporterRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            rscClassRef,
            kindRef,
            layerDataHelperProviderRef
        );
    }

    protected boolean genericNeedsCacheDevice(Resource rsc, List<DeviceLayerKind> remainingLayerListRef)
    {
        boolean isNvmeBelow = remainingLayerListRef.contains(DeviceLayerKind.NVME);
        StateFlags<Flags> rscflags = rsc.getStateFlags();
        boolean isNvmeInitiator = rscflags.isSet(Resource.Flags.NVME_INITIATOR);
        boolean isEbsInitiator = rscflags.isSet(Resource.Flags.EBS_INITIATOR);
        boolean isDrbdDiskless = rscflags.isSet(Resource.Flags.DRBD_DISKLESS);

        boolean needsCacheDevice;
        if (isDrbdDiskless)
        {
            needsCacheDevice = false;
        }
        else
        {
            needsCacheDevice = (isNvmeInitiator && isNvmeBelow) ||
                (!isNvmeBelow && !isNvmeInitiator) ||
                isEbsInitiator;
        }
        return needsCacheDevice;
    }
}
