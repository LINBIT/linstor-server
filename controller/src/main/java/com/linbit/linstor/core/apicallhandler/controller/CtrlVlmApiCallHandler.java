package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlVlmApiCallHandler
{
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResponseConverter responseConverter;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final Provider<Peer> peer;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final BackupInfoManager backupInfoMgr;

    @Inject
    public CtrlVlmApiCallHandler(
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        BackupInfoManager backupInfoMgrRef
    )
    {
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        backupInfoMgr = backupInfoMgrRef;
    }

    public Flux<ApiCallRc> modify(
        UUID vlmUuid,
        String nodeNameStr,
        String rscNameStr,
        Integer vlmNrInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces
    )
    {
        ResponseContext context = makeVlmContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr,
            vlmNrInt
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify volume",
                lockGuardFactory.buildDeferred(
                    WRITE,
                    NODES_MAP, RSC_DFN_MAP, STOR_POOL_DFN_MAP
                ),
                () -> modifyInTransaction(
                    vlmUuid,
                    nodeNameStr,
                    rscNameStr,
                    vlmNrInt,
                    overrideProps,
                    deletePropKeys,
                    deletePropNamespaces,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyInTransaction(
        UUID vlmUuid,
        String nodeNameStr,
        String rscNameStr,
        Integer vlmNrInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces,
        ResponseContext context
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        boolean notifyStlts;

        try
        {
            Volume vlm = ctrlApiDataLoader.loadVlm(nodeNameStr, rscNameStr, vlmNrInt, true);

            if (vlmUuid != null && !vlmUuid.equals(vlm.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_VLM,
                    "UUID-check failed"
                ));
            }
            if (backupInfoMgr.restoreContainsRscDfn(vlm.getResourceDefinition()))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_IN_USE,
                        rscNameStr + " is currently being restored from a backup. " +
                            "Please wait until the restore is finished"
                    )
                );
            }

            Props props = ctrlPropsHelper.getProps(vlm);

            notifyStlts = ctrlPropsHelper.fillProperties(
                apiCallRcs, LinStorObject.VLM, overrideProps, props, ApiConsts.FAIL_ACC_DENIED_VLM);
            notifyStlts = ctrlPropsHelper.remove(
                apiCallRcs, LinStorObject.VLM, props, deletePropKeys, deletePropNamespaces) || notifyStlts;

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(
                apiCallRcs,
                context,
                ApiSuccessUtils.defaultModifiedEntry(vlm.getUuid(), getVlmDescriptionInline(vlm))
            );

            if (notifyStlts)
            {
                flux = ctrlSatelliteUpdateCaller
                    .updateSatellites(vlm.getResourceDefinition(), Flux.empty())
                    .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2());
            }
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs).concatWith(flux);
    }

    public static String getVlmDescription(Volume vlm)
    {
        return getVlmDescription(vlm.getAbsResource(), vlm.getVolumeDefinition());
    }

    public static String getVlmDescription(Resource rsc, VolumeDefinition vlmDfn)
    {
        return getVlmDescription(
            rsc.getNode().getName().displayValue,
            rsc.getResourceDefinition().getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
    }

    public static String getVlmDescription(NodeName nodeName, ResourceName rscName, VolumeNumber vlmNr)
    {
        return getVlmDescription(nodeName.getDisplayName(), rscName.displayValue, vlmNr.value);
    }

    public static String getVlmDescription(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "Volume with volume number '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" +
            nodeNameStr + "'";
    }

    public static String getVlmDescriptionInline(Volume vlm)
    {
        return getVlmDescriptionInline(vlm.getAbsResource(), vlm.getVolumeDefinition());
    }

    public static String getVlmDescriptionInline(Resource rsc, VolumeDefinition vlmDfn)
    {
        return getVlmDescriptionInline(
            rsc.getNode().getName().displayValue,
            rsc.getResourceDefinition().getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
    }

    public static String getVlmDescriptionInline(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "volume with volume number '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" +
            nodeNameStr + "'";
    }

    static ResponseContext makeVlmContext(
        ApiOperation operation,
        String nodeNameStr,
        String rscNameStr,
        Integer vlmNr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        objRefs.put(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr));

        return new ResponseContext(
            operation,
            getVlmDescription(nodeNameStr, rscNameStr, vlmNr),
            getVlmDescriptionInline(nodeNameStr, rscNameStr, vlmNr),
            ApiConsts.MASK_VLM,
            objRefs
        );
    }
}
