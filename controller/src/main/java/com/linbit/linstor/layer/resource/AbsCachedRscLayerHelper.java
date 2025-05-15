package com.linbit.linstor.layer.resource;

import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Provider;

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
        AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        DynamicNumberPool layerRscIdPoolRef,
        Class<RSC_LO> rscClassRef,
        DeviceLayerKind kindRef,
        Provider<CtrlRscLayerDataFactory> layerDataHelperProviderRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            rscClassRef,
            kindRef,
            layerDataHelperProviderRef
        );
    }

    protected boolean genericNeedsCacheDevice(Resource rsc, List<DeviceLayerKind> remainingLayerListRef)
        throws AccessDeniedException
    {
        boolean isNvmeBelow = remainingLayerListRef.contains(DeviceLayerKind.NVME);
        StateFlags<Flags> rscflags = rsc.getStateFlags();
        boolean isNvmeInitiator = rscflags.isSet(apiCtx, Resource.Flags.NVME_INITIATOR);
        boolean isEbsInitiator = rscflags.isSet(apiCtx, Resource.Flags.EBS_INITIATOR);
        boolean isDrbdDiskless = rscflags.isSet(apiCtx, Resource.Flags.DRBD_DISKLESS);

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
