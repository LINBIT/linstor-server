package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperInternalState;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.AutoSelectorConfig;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashSet;
import java.util.Iterator;

@Singleton
public class CtrlRscAutoRePlaceRscHelper implements AutoHelper
{
    private final Provider<AccessContext> accCtx;
    private final SystemConfRepository systemConfRepo;
    private final HashSet<ResourceDefinition> needRePlaceRsc = new HashSet<ResourceDefinition>();
    private final CtrlRscAutoPlaceApiCallHandler autoPlaceHandler;

    @Inject
    public CtrlRscAutoRePlaceRscHelper(
        @PeerContext Provider<AccessContext> accCtxRef,
        SystemConfRepository systemConfRepoRef,
        CtrlRscAutoPlaceApiCallHandler autoPlaceHandlerRef
    )
    {
        accCtx = accCtxRef;
        systemConfRepo = systemConfRepoRef;
        autoPlaceHandler = autoPlaceHandlerRef;
    }

    @Override
    public void manage(
        ApiCallRcImpl apiCallRcImpl,
        ResourceDefinition rscDfn,
        AutoHelperInternalState autoHelperInternalState
    )
    {
        if (needRePlaceRsc.contains(rscDfn))
        {
            PriorityProps props;
            int minReplicaCount;
            int placeCount;
            int curReplicaCount = 0;
            try
            {
                props = new PriorityProps(
                    rscDfn.getProps(accCtx.get()),
                    rscDfn.getResourceGroup().getProps(accCtx.get()),
                    systemConfRepo.getCtrlConfForView(accCtx.get())
                );
                placeCount = rscDfn.getResourceGroup().getAutoPlaceConfig().getReplicaCount(accCtx.get());
                minReplicaCount = Integer.parseInt(
                    props.getProp(
                        ApiConsts.KEY_MIN_REPLICA_COUNT,
                        "",
                        "" + placeCount
                    )
                );
                if (placeCount < minReplicaCount) // minReplicaCount should be smaller than placeCount
                {
                    minReplicaCount = placeCount;
                }
                Iterator<Resource> itres = rscDfn.iterateResource(accCtx.get());
                while (itres.hasNext())
                {
                    Resource res = itres.next();
                    StateFlags<Flags> flags = res.getStateFlags();
                    if (
                        !flags.isSomeSet(
                            accCtx.get(), Resource.Flags.DELETE, Resource.Flags.TIE_BREAKER,
                            Resource.Flags.DRBD_DISKLESS
                        ) &&
                            LayerRscUtils.getLayerStack(res, accCtx.get()).contains(DeviceLayerKind.DRBD)
                    )
                    {
                        curReplicaCount++;
                    }
                }
                if (curReplicaCount < minReplicaCount)
                {

                    AutoSelectorConfig autoPlaceConfig = rscDfn.getResourceGroup().getAutoPlaceConfig();
                    AutoSelectFilterApi selectFilter = new AutoSelectFilterPojo(
                        minReplicaCount,
                        autoPlaceConfig.getNodeNameList(accCtx.get()),
                        autoPlaceConfig.getStorPoolNameList(accCtx.get()),
                        autoPlaceConfig.getDoNotPlaceWithRscList(accCtx.get()),
                        autoPlaceConfig.getDoNotPlaceWithRscRegex(accCtx.get()),
                        autoPlaceConfig.getReplicasOnSameList(accCtx.get()),
                        autoPlaceConfig.getReplicasOnDifferentList(accCtx.get()),
                        autoPlaceConfig.getLayerStackList(accCtx.get()),
                        autoPlaceConfig.getProviderList(accCtx.get()),
                        autoPlaceConfig.getDisklessOnRemaining(accCtx.get()),
                        null
                    );
                    try
                    {
                        autoHelperInternalState.additionalFluxList
                            .add(autoPlaceHandler.autoPlace(rscDfn.getName().toString(), selectFilter)
                                .doOnComplete(() -> needRePlaceRsc.remove(rscDfn))
                            );
                        autoHelperInternalState.requiresUpdateFlux = true;
                    }
                    catch (ApiRcException exc)
                    {
                        // Ignored, try again later
                    }
                }
                else
                {
                    needRePlaceRsc.remove(rscDfn);
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ApiAccessDeniedException(
                    exc,
                    "accessing flags of " + rscDfn.getName(),
                    ApiConsts.FAIL_ACC_DENIED_RSC_DFN
                );
            }
        }
    }

    public void addNeedRePlaceRsc(Resource rsc)
    {
        needRePlaceRsc.add(rsc.getDefinition());
    }
}
