package com.linbit.linstor.core.apicallhandler.controller.autohelper;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.utils.ProcCryptoUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.ProcCryptoEntry;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.StringUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public class CtrlRscDfnAutoVerifyAlgoHelper implements AutoHelper
{
    private final ErrorReporter errorReporter;
    private final SystemConfRepository sysCfgRepo;

    @Inject
    public CtrlRscDfnAutoVerifyAlgoHelper(
        ErrorReporter errorReporterRef,
        SystemConfRepository systemConfRepositoryRef)
    {
        errorReporter = errorReporterRef;
        sysCfgRepo = systemConfRepositoryRef;
    }

    @Override
    public AutoHelperType getType()
    {
        return AutoHelperType.VERIFY_ALGORITHM;
    }

    @Override
    public void manage(AutoHelperContext ctx)
    {
        ctx.responses.addEntries(checkVerifyAlgorithm(ctx.rscDfn));
        PairNonNull<ApiCallRc, Set<Resource>> result = updateVerifyAlgorithm(ctx.rscDfn);
        ctx.responses.addEntries(result.objA);
        if (!result.objB.isEmpty())
        {
            // the verify-alg prop changed, so all already deployed nodes need an updated .res file, otherwise
            // only satellites that happen to receive an update for other reasons would apply the new algorithm,
            // leaving the DRBD connections between updated and not updated nodes in StandAlone
            ctx.requiresUpdateFlux = true;
        }
    }

    private Map<String, List<ProcCryptoEntry>> getCryptoEntryMap(ResourceDefinition rscDfn)
    {
        return rscDfn.streamResource()
            .filter(
                rsc ->
                {
                    boolean result = false;
                    Peer peer = rsc.getNode().getPeer();
                    if (!rsc.getNode().isDeleted() && peer.isFullSyncApplied())
                    {
                        result = LayerRscUtils.getLayerStack(rsc)
                            .contains(DeviceLayerKind.DRBD);
                    }
                    return result;
                }
            )
            .collect(Collectors.toMap(
                rsc -> rsc.getNode().getName().displayValue,
                rsc -> rsc.getNode().getSupportedCryptos()));
    }

    /**
     * Returns {@code true} only if every node that has this resource definition deployed with a DRBD layer is
     * currently connected and has finished its full-sync (and has therefore already reported its supported crypto
     * algorithms).
     *
     * <p>The DRBD verify algorithm must be identical on all nodes of a resource. The auto-selection derives it from
     * the intersection of the crypto algorithms supported by all nodes. If this is recalculated while only a subset
     * of the nodes has reported its crypto entries - which happens for example during controller startup, when the
     * satellites reconnect and finish their full-sync one after another - the intersection (and therefore the selected
     * algorithm) can change with every additional node that reports. Pushing such an intermediate algorithm to the
     * already deployed resource and then correcting it a moment later makes {@code drbdadm adjust} change the
     * connection's 'verify-alg', which forces the affected DRBD connection into StandAlone without an automatic
     * reconnect.</p>
     *
     * <p>Waiting until all DRBD nodes have reported avoids these intermediate values and keeps the computed algorithm
     * stable.</p>
     */
    private boolean allDrbdNodesReportedCryptos(ResourceDefinition rscDfn)
    {
        boolean allReported = true;
        for (Resource rsc : rscDfn.streamResource().collect(Collectors.toList()))
        {
            if (!rsc.getNode().isDeleted() &&
                LayerRscUtils.getLayerStack(rsc).contains(DeviceLayerKind.DRBD))
            {
                final Peer peer = rsc.getNode().getPeer();
                if (!peer.isFullSyncApplied())
                {
                    allReported = false;
                    break;
                }
            }
        }
        return allReported;
    }

    private ApiCallRc checkVerifyAlgorithm(ResourceDefinition rscDfn)
    {
        final ApiCallRcImpl rc = new ApiCallRcImpl();

        PriorityProps prioProps = new PriorityProps()
            .addProps(rscDfn.getProps(), "RD (" + rscDfn.getName() + ")")
            .addProps(
                rscDfn.getResourceGroup().getProps(),
                "RG (" + rscDfn.getResourceGroup().getName() + ")")
            .addProps(sysCfgRepo.getCtrlConfForView(), "C");

        final @Nullable String verifyAlgo = prioProps.getProp(
            InternalApiConsts.DRBD_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_NET_OPTIONS);

        if (verifyAlgo != null && rscDfn.usesLayer(DeviceLayerKind.DRBD))
        {
            final Map<String, List<ProcCryptoEntry>> nodeCryptos = getCryptoEntryMap(rscDfn);

            if (!nodeCryptos.isEmpty() &&
                !ProcCryptoUtils.cryptoDriverSupported(nodeCryptos, ProcCryptoEntry.CryptoType.SHASH, verifyAlgo))
            {
                throw new ApiRcException(ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.FAIL_INVLD_PROP,
                    String.format("Resource '%s': Verify algorithm '%s' not supported on all nodes."
                        , rscDfn.getName(), verifyAlgo)));
            }
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
     */
    public PairNonNull<ApiCallRc, Set<Resource>> updateVerifyAlgorithm(ResourceDefinition rscDfn)
    {
        final ApiCallRcImpl rc = new ApiCallRcImpl();
        final Set<Resource> touchedResources = new HashSet<>();

        try
        {
            final PriorityProps prioProps = new PriorityProps(
                rscDfn.getProps(),
                rscDfn.getResourceGroup().getProps(),
                sysCfgRepo.getCtrlConfForView()
            );

            final @Nullable String disableAuto = prioProps.getProp(ApiConsts.KEY_DRBD_DISABLE_AUTO_VERIFY_ALGO,
                ApiConsts.NAMESPC_DRBD_OPTIONS);
            final boolean autoVerifyAlgoEnabled = StringUtils.propFalseOrNull(disableAuto) &&
                rscDfn.usesLayer(DeviceLayerKind.DRBD);

            // Only (re)compute once every DRBD node of the resource definition has finished its full-sync and has
            // therefore reported its supported crypto algorithms. Computing over a subset of the nodes (e.g. while
            // satellites are still reconnecting after a controller restart) can select an intermediate algorithm that
            // gets corrected once the remaining nodes report - and applying that correction to an already
            // connecting/connected DRBD resource forces the connection into StandAlone. While nodes are still missing
            // neither branch runs, so the current value is kept until all nodes have reported.
            if (autoVerifyAlgoEnabled && allDrbdNodesReportedCryptos(rscDfn))
            {
                final Map<String, List<ProcCryptoEntry>> nodeCryptos = getCryptoEntryMap(rscDfn);

                final String allowedAutoAlgosString = sysCfgRepo.getCtrlConfForView()
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

                final @Nullable ProcCryptoEntry commonHashAlgo = ProcCryptoUtils.commonCryptoType(
                    nodeCryptos, ProcCryptoEntry.CryptoType.SHASH, allowedAlgos
                );

                if (commonHashAlgo != null)
                {
                    final @Nullable String autoVerifyAlgo = rscDfn.getProps().getProp(
                        InternalApiConsts.DRBD_AUTO_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_OPTIONS
                    );

                    if (!commonHashAlgo.getName().equalsIgnoreCase(autoVerifyAlgo))
                    {
                        errorReporter.logInfo(
                            "Drbd-auto-verify-Algo for %s automatically set to %s",
                            rscDfn.getName(),
                            commonHashAlgo.getName()
                        );
                        rscDfn.getProps().setProp(
                            InternalApiConsts.DRBD_AUTO_VERIFY_ALGO,
                            commonHashAlgo.getName(),
                            ApiConsts.NAMESPC_DRBD_OPTIONS
                        );
                        touchedResources.addAll(rscDfn.streamResource().collect(Collectors.toList()));
                        rc.addEntry(
                            String.format("Updated %s DRBD auto verify algorithm to '%s'",
                                rscDfn.getName(), commonHashAlgo.getName()),
                            ApiConsts.MASK_INFO
                        );
                    }
                }
                else
                {
                    if (nodeCryptos.size() > 1)
                    {
                        final String msg = String.format(
                            "No common DRBD verify algorithm found for '%s', clearing prop",
                            rscDfn.getName());
                        errorReporter.logInfo(msg);
                        final Props rscDfnProps = rscDfn.getProps();
                        rscDfnProps.removeProp(InternalApiConsts.DRBD_AUTO_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_OPTIONS);
                        touchedResources.addAll(rscDfn.streamResource().toList());
                    }
                }
            }
            else if (!autoVerifyAlgoEnabled)
            {
                // Auto Verify Algo is disabled, so delete the property if it is set
                final Props rscDfnProps = rscDfn.getProps();
                if (rscDfnProps.getProp(
                        InternalApiConsts.DRBD_AUTO_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_OPTIONS) != null)
                {
                    rscDfnProps.removeProp(InternalApiConsts.DRBD_AUTO_VERIFY_ALGO, ApiConsts.NAMESPC_DRBD_OPTIONS);
                    touchedResources.addAll(rscDfn.streamResource().collect(Collectors.toList()));
                }
            }
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (InvalidValueException invValExc)
        {
            throw new ApiException(invValExc);
        }

        return new PairNonNull<>(rc, touchedResources);
    }
}
