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
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.utils.ProcCryptoUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.ProcCryptoEntry;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public class CtrlRscDfnAutoVerifyAlgoHelper implements CtrlRscAutoHelper.AutoHelper
{
    private final ErrorReporter errorReporter;
    private final Provider<AccessContext> peerCtxProvider;
    private final SystemConfRepository sysCfgRepo;

    @Inject
    public CtrlRscDfnAutoVerifyAlgoHelper(
        ErrorReporter errorReporterRef,
        @PeerContext Provider<AccessContext> peerCtxProviderRef,
        SystemConfRepository systemConfRepositoryRef)
    {
        errorReporter = errorReporterRef;
        this.peerCtxProvider = peerCtxProviderRef;
        sysCfgRepo = systemConfRepositoryRef;
    }

    @Override
    public CtrlRscAutoHelper.AutoHelperType getType()
    {
        return CtrlRscAutoHelper.AutoHelperType.VerifyAlgorithm;
    }

    @Override
    public void manage(CtrlRscAutoHelper.AutoHelperContext ctx)
    {
        ctx.responses.addEntries(checkVerifyAlgorithm(ctx.rscDfn));
        ctx.responses.addEntries(updateVerifyAlgorithm(ctx.rscDfn).objA);
    }

    private Map<String, List<ProcCryptoEntry>> getCryptoEntryMap(ResourceDefinition rscDfn)
            throws AccessDeniedException
    {
        return rscDfn.streamResource(peerCtxProvider.get())
            .filter(
                rsc ->
                {
                    boolean result = false;
                    try
                    {
                        if (!rsc.getNode().isDeleted())
                        {
                            result = LayerRscUtils.getLayerStack(rsc, peerCtxProvider.get())
                                .contains(DeviceLayerKind.DRBD);
                        }
                    }
                    catch (AccessDeniedException ignored)
                    {
                    }
                    return result;
                }
            )
            .collect(Collectors.toMap(
                rsc -> rsc.getNode().getName().displayValue,
                rsc -> rsc.getNode().getSupportedCryptos()));
    }

    private ApiCallRc checkVerifyAlgorithm(ResourceDefinition rscDfn)
    {
        final ApiCallRcImpl rc = new ApiCallRcImpl();

        try
        {
            PriorityProps prioProps = new PriorityProps()
                .addProps(rscDfn.getProps(peerCtxProvider.get()), "RD (" + rscDfn.getName() + ")")
                .addProps(
                    rscDfn.getResourceGroup().getProps(peerCtxProvider.get()),
                    "RG (" + rscDfn.getResourceGroup().getName() + ")")
                .addProps(sysCfgRepo.getCtrlConfForView(peerCtxProvider.get()), "C");

            final String verifyAlgo = prioProps.getProp(
                InternalApiConsts.DRBD_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);

            if (verifyAlgo != null && rscDfn.usesLayer(peerCtxProvider.get(), DeviceLayerKind.DRBD))
            {
                final Map<String, List<ProcCryptoEntry>> nodeCryptos = getCryptoEntryMap(rscDfn);

                if (!nodeCryptos.isEmpty() &&
                    !ProcCryptoUtils.cryptoDriverSupported(nodeCryptos, ProcCryptoEntry.CryptoType.SHASH, verifyAlgo))
                {
                    throw new ApiRcException(ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_INVLD_PROP,
                        String.format("Verify algorithm '%s' not supported on all nodes.", verifyAlgo)));
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "reading verify algorithm",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }

        return rc;
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
    public Pair<ApiCallRc, Set<Resource>> updateVerifyAlgorithm(ResourceDefinition rscDfn)
    {
        final ApiCallRcImpl rc = new ApiCallRcImpl();
        final Set<Resource> touchedResources = new HashSet<>();

        final AccessContext peerCtx = peerCtxProvider.get();
        try
        {
            final PriorityProps prioProps = new PriorityProps(
                rscDfn.getProps(peerCtx),
                rscDfn.getResourceGroup().getProps(peerCtx),
                sysCfgRepo.getCtrlConfForView(peerCtx)
            );

            final String disableAuto = prioProps.getProp(ApiConsts.KEY_DRBD_DISABLE_AUTO_VERIFY_ALGO,
                ApiConsts.NAMESPC_DRBD_OPTIONS);
            if (StringUtils.propFalseOrNull(disableAuto) &&
                rscDfn.usesLayer(peerCtx, DeviceLayerKind.DRBD))
            {
                final Map<String, List<ProcCryptoEntry>> nodeCryptos = getCryptoEntryMap(rscDfn);

                final String allowedAutoAlgosString = sysCfgRepo.getCtrlConfForView(peerCtx)
                        .getPropWithDefault(
                            InternalApiConsts.KEY_DRBD_AUTO_VERIFY_ALGO_ALLOWED_LIST,
                            ApiConsts.NAMESPC_DRBD_OPTIONS,
                            "");
                ArrayList<String> allowedAlgos = new ArrayList<>(
                    Arrays.asList(allowedAutoAlgosString.trim().split(";"))
                );

                final String allowedAutoAlgosUserString = prioProps.getProp(
                    ApiConsts.KEY_DRBD_AUTO_VERIFY_ALGO_ALLOWED_USER, ApiConsts.NAMESPC_DRBD_OPTIONS, ""
                );
                allowedAlgos.addAll(Arrays.asList(allowedAutoAlgosUserString.trim().split(";")));

                final ProcCryptoEntry commonHashAlgo = ProcCryptoUtils.commonCryptoType(
                    nodeCryptos, ProcCryptoEntry.CryptoType.SHASH, allowedAlgos
                );

                if (commonHashAlgo != null)
                {
                    final String autoVerifyAlgo = rscDfn.getProps(peerCtx).getProp(
                        InternalApiConsts.DRBD_AUTO_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_OPTIONS
                    );

                    if (!commonHashAlgo.getName().equalsIgnoreCase(autoVerifyAlgo))
                    {
                        errorReporter.logInfo(
                            "Drbd-auto-verify-Algo for %s automatically set to %s",
                            rscDfn.getName(),
                            commonHashAlgo.getName()
                        );
                        rscDfn.getProps(peerCtxProvider.get()).setProp(
                            InternalApiConsts.DRBD_AUTO_VERIFY_ALGO,
                            commonHashAlgo.getName(),
                            ApiConsts.NAMESPC_DRBD_OPTIONS
                        );
                        touchedResources.addAll(rscDfn.streamResource(
                            peerCtxProvider.get()).collect(Collectors.toList()));
                        rc.addEntry(
                            String.format("Updated %s DRBD auto verify algorithm to '%s'",
                                rscDfn.getName(), commonHashAlgo.getName()),
                            ApiConsts.MASK_INFO
                        );
                    }
                }
                else
                {
                    final String msg = String.format("No common DRBD verify algorithm found for '%s', clearing prop",
                        rscDfn.getName());
                    errorReporter.logInfo(msg);
                    final Props rscDfnProps = rscDfn.getProps(peerCtx);
                    rscDfnProps.removeProp(InternalApiConsts.DRBD_AUTO_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_OPTIONS);
                    touchedResources.addAll(rscDfn.streamResource(
                        peerCtxProvider.get()).collect(Collectors.toList()));
                }
            }
            else
            {
                // Auto Verify Algo is disabled, so delete the property if it is set
                final Props rscDfnProps = rscDfn.getProps(peerCtx);
                if (rscDfnProps.getProp(
                        InternalApiConsts.DRBD_AUTO_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_OPTIONS) != null)
                {
                    rscDfnProps.removeProp(InternalApiConsts.DRBD_AUTO_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_OPTIONS);
                    touchedResources.addAll(rscDfn.streamResource(
                        peerCtxProvider.get()).collect(Collectors.toList()));
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "setting verify algorithm",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (InvalidValueException invValExc)
        {
            throw new ApiException(invValExc);
        }

        return new Pair<>(rc, touchedResources);
    }
}
