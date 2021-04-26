package com.linbit.linstor.core.apicallhandler.controller.autoplacer.strategies;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.AutoplaceStrategy;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class MaximumThroughputRemainingStrategy implements AutoplaceStrategy
{
    private final AccessContext apiCtx;
    private final SystemConfRepository sysCfgRepo;

    public MaximumThroughputRemainingStrategy(
        @ApiContext AccessContext apiCtxRef,
        SystemConfRepository sysCfgRepoRef
    )
    {
        apiCtx = apiCtxRef;
        sysCfgRepo = sysCfgRepoRef;
    }

    @Override
    public Map<StorPool, Double> rate(Collection<StorPool> storPoolsRef, RatingAdditionalInfo additionalInfoRef)
        throws AccessDeniedException
    {
        Map<StorPool, Double> ret = new HashMap<>();
        for (StorPool sp : storPoolsRef)
        {
            double remainingThroughput = getMaxThroughput(sp);

            // only count distinct volumes
            HashSet<Volume> vlms = new HashSet<>();
            for (VlmProviderObject<Resource> vlmObj : sp.getVolumes(apiCtx))
            {
                Volume vlm = (Volume) vlmObj.getVolume();
                if (vlms.add(vlm))
                {
                    remainingThroughput -= getThroughput(vlm);
                }
            }
            ret.put(sp, remainingThroughput);
        }
        return ret;
    }


    private double getMaxThroughput(StorPool spRef)
        throws AccessDeniedException
    {
        String val = new PriorityProps(
            spRef.getProps(apiCtx),
            spRef.getDefinition(apiCtx).getProps(apiCtx),
            sysCfgRepo.getCtrlConfForView(apiCtx)
        ).getProp(
            ApiConsts.KEY_AUTOPLACE_MAX_THROUGHPUT,
            ApiConsts.NAMESPC_AUTOPLACER,
            "0.0"
        );
        return parseDouble(val, spRef);
    }

    private double parseDouble(String valRef, Object parentObj)
    {
        double val;
        try
        {
            val = Double.parseDouble(valRef);
        }
        catch (NumberFormatException nfExc)
        {
            throw new ApiException("Failed parse '" + valRef + "' from " + parentObj.toString(), nfExc);
        }
        return val;
    }

    private double getThroughput(Volume vlm) throws AccessDeniedException
    {
        PriorityProps prioProps = new PriorityProps(
            vlm.getProps(apiCtx),
            vlm.getAbsResource().getProps(apiCtx),
            vlm.getAbsResource().getNode().getProps(apiCtx),
            vlm.getVolumeDefinition().getProps(apiCtx),
            vlm.getResourceDefinition().getProps(apiCtx),
            vlm.getResourceDefinition().getResourceGroup().getVolumeGroupProps(apiCtx, vlm.getVolumeNumber()),
            vlm.getResourceDefinition().getResourceGroup().getProps(apiCtx),
            sysCfgRepo.getCtrlConfForView(apiCtx)
        );
        String readVal = prioProps.getProp(ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_READ, ApiConsts.NAMESPC_SYS_FS, "0.0");
        String writeVal = prioProps.getProp(ApiConsts.KEY_SYS_FS_BLKIO_THROTTLE_WRITE, ApiConsts.NAMESPC_SYS_FS, "0.0");

        double read = parseDouble(readVal, vlm);
        double write = parseDouble(writeVal, vlm);

        return read + write;
    }

    @Override
    public String getName()
    {
        return ApiConsts.KEY_AUTOPLACE_MAX_THROUGHPUT;
    }

    @Override
    public MinMax getMinMax()
    {
        return MinMax.MAXIMIZE;
    }

}
