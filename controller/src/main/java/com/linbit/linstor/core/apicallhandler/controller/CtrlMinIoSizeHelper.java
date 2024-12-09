package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;

import java.util.Iterator;

public class CtrlMinIoSizeHelper
{
    private SystemConfRepository sysConfRepo;

    @Inject
    public CtrlMinIoSizeHelper(final SystemConfRepository sysConfRepoRef)
    {
        sysConfRepo = sysConfRepoRef;
    }

    /**
     * Indicates whether all volume definitions of the specified resource definition allow automatic
     * min-io-size determination.
     *
     * @param rscDfn Resource definition to check
     * @param accCtx Access context for accessing information
     * @return true if all volumes allow automatic min-io-size determination
     * @throws AccessDeniedException If access to required information is denied
     */
    public boolean isAutoMinIoSize(final ResourceDefinition rscDfn, final AccessContext accCtx)
        throws AccessDeniedException
    {
        boolean autoFlag = true;
        Iterator<VolumeDefinition> vlmDfnIter = rscDfn.iterateVolumeDfn(accCtx);
        while (vlmDfnIter.hasNext())
        {
            final VolumeDefinition vlmDfn = vlmDfnIter.next();
            if (!isAutoMinIoSize(vlmDfn, accCtx))
            {
                autoFlag = false;
            }
        }
        return autoFlag;
    }

    /**
     * Indicates whether the specified volume definition allows automatic min-io-size determination.
     *
     * @param vlmDfn Volume definition to check
     * @param accCtx Access context for accessing information
     * @return true if the volumes allows automatic min-io-size determination
     * @throws AccessDeniedException If access to required information is denied
     */
    public boolean isAutoMinIoSize(final VolumeDefinition vlmDfn, final AccessContext accCtx)
        throws AccessDeniedException
    {
        final ReadOnlyProps ctrlProps = sysConfRepo.getCtrlConfForView(accCtx);
        final ResourceDefinition rscDfn = vlmDfn.getResourceDefinition();
        final ResourceGroup rscGrp = rscDfn.getResourceGroup();
        final Props rscDfnProps = rscDfn.getProps(accCtx);
        final Props vlmDfnProps = vlmDfn.getProps(accCtx);
        final PriorityProps prioProps = new PriorityProps(
            vlmDfnProps,
            rscDfnProps,
            rscGrp.getVolumeGroupProps(accCtx, vlmDfn.getVolumeNumber()),
            rscGrp.getProps(accCtx),
            ctrlProps
        );
        final String autoFlagStr = prioProps.getProp(
            ApiConsts.KEY_DRBD_AUTO_BLOCK_SIZE,
            ApiConsts.NAMESPC_LINSTOR_DRBD,
            Boolean.TRUE.toString()
        );
        return Boolean.parseBoolean(autoFlagStr);
    }
}
