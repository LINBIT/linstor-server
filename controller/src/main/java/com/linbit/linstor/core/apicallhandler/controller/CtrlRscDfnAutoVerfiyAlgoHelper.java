package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.utils.ProcCryptoUtils;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.ProcCryptoEntry;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class CtrlRscDfnAutoVerfiyAlgoHelper implements CtrlRscAutoHelper.AutoHelper {
    private final Provider<AccessContext> peerCtxProvider;
    private final SystemConfRepository sysCfgRepo;

    @Inject
    public CtrlRscDfnAutoVerfiyAlgoHelper(
        @PeerContext Provider<AccessContext> peerCtxProviderRef,
        SystemConfRepository systemConfRepositoryRef)
    {
        this.peerCtxProvider = peerCtxProviderRef;
        sysCfgRepo = systemConfRepositoryRef;
    }

    @Override
    public void manage(CtrlRscAutoHelper.AutoHelperContext ctx)
    {
        ctx.responses.addEntries(updateVerifyAlgorithm(ctx.rscDfn));
    }

    /**
     * Checks if auto verify algorithm setting is enabled and if so
     * will check all diskful nodes for their common shash algorithm with the highest priority.
     * If the found algorithm is different than the current(might be null) it will set the DRBD property for it.
     *
     * @param rscDfn Resource definition to check for drbd verify algorithm
     * @return ApiCallRc with the update message if property was changed, else empty
     * @throws ApiException If an invalid value would be set
     * @throws ApiDatabaseException if setProp fails
     * @throws ApiAccessDeniedException if apiCtx doesn't have access to resource definition
     */
    private ApiCallRc updateVerifyAlgorithm(ResourceDefinition rscDfn) {
        final ApiCallRcImpl rc = new ApiCallRcImpl();

        try
        {
            final PriorityProps prioProps = new PriorityProps(
                rscDfn.getProps(peerCtxProvider.get()),
                rscDfn.getResourceGroup().getProps(peerCtxProvider.get()),
                sysCfgRepo.getCtrlConfForView(peerCtxProvider.get())
            );

            final String disableAuto = prioProps.getProp(ApiConsts.KEY_DRBD_DISABLE_AUTO_VERIFY_ALGO,
                ApiConsts.NAMESPC_DRBD_OPTIONS);
            if (disableAuto == null && rscDfn.getLayerStack(peerCtxProvider.get()).contains(DeviceLayerKind.DRBD))
            {
                final Map<String, List<ProcCryptoEntry>> nodeCryptos = new HashMap<>();
                rscDfn.streamResource(peerCtxProvider.get())
                    .filter(rsc -> {
                        try {
                            if (!rsc.getNode().isDeleted())
                            {
                                return !rsc.isDrbdDiskless(peerCtxProvider.get()) &&
                                    LayerRscUtils.getLayerStack(rsc, peerCtxProvider.get()).contains(DeviceLayerKind.DRBD);
                            }
                        } catch (AccessDeniedException ignored) {}
                        return false;
                    }).forEach(rsc ->
                        nodeCryptos.put(rsc.getNode().getName().displayValue, rsc.getNode().getSupportedCryptos()));

                final ProcCryptoEntry commonHashAlgo = ProcCryptoUtils.commonCryptoType(
                    nodeCryptos, ProcCryptoEntry.CryptoType.SHASH);

                final String verifyAlgo = rscDfn.getProps(peerCtxProvider.get()).getProp(
                    InternalApiConsts.DRBD_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
                if (commonHashAlgo != null)
                {
                    if (!commonHashAlgo.getDriver().equals(verifyAlgo))
                    {
                        rscDfn.getProps(peerCtxProvider.get()).setProp(
                            InternalApiConsts.DRBD_VERIFY_ALGO,
                            commonHashAlgo.getDriver(),
                            ApiConsts.NAMESPC_DRBD_NET_OPTIONS);
                        rc.addEntry(
                            String.format("Updated DRBD verify algorithm to '%s'", commonHashAlgo.getDriver()),
                            ApiConsts.MASK_INFO);
                    }
                }
            }
        } catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "setting verify algorithm",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        } catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        } catch (InvalidValueException invValExc)
        {
            throw new ApiException(invValExc);
        }

        return rc;
    }
}
