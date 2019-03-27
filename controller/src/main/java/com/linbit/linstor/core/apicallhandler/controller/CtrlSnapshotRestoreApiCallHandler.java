package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.OperationDescription;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.event.EventStreamClosedException;
import com.linbit.linstor.event.EventStreamTimeoutException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlSnapshotRestoreApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSnapshotHelper ctrlSnapshotHelper;
    private final CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;

    @Inject
    public CtrlSnapshotRestoreApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlRscCrtApiHelper ctrlRscCrtApiHelperRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlRscCrtApiHelper = ctrlRscCrtApiHelperRef;
        ctrlVlmCrtApiHelper = ctrlVlmCrtApiHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        lockGuardFactory = lockGuardFactoryRef;
    }

    private ResponseContext makeSnapshotRestoreContext(String rscNameStr)
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        return new ResponseContext(
            ApiOperation.makeRegisterOperation(),
            "Resource: '" + rscNameStr + "'",
            "resource '" + rscNameStr,
            ApiConsts.MASK_SNAPSHOT,
            objRefs
        );
    }

    public Flux<ApiCallRc> restoreSnapshot(
        List<String> nodeNameStrs,
        String fromRscNameStr,
        String fromSnapshotNameStr,
        String toRscNameStr
    )
    {
        ResponseContext context = makeSnapshotRestoreContext(toRscNameStr);
        return scopeRunner.fluxInTransactionalScope(
            "Restore Snapshot Resource",
            lockGuardFactory.createDeferred()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .build(),
            () -> restoreResourceInTransaction(nodeNameStrs, fromRscNameStr, fromSnapshotNameStr, toRscNameStr)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> restoreResourceInTransaction(
        List<String> nodeNameStrs,
        String fromRscNameStr,
        String fromSnapshotNameStr,
        String toRscNameStr
    )
    {
        Flux<ApiCallRc> deploymentResponses = Flux.just();
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = new ResponseContext(
            new ApiOperation(ApiConsts.MASK_CRT, new OperationDescription("restore", "restoring")),
            getSnapshotRestoreDescription(nodeNameStrs, toRscNameStr),
            getSnapshotRestoreDescriptionInline(nodeNameStrs, toRscNameStr),
            ApiConsts.MASK_RSC,
            Collections.emptyMap()
        );

        try
        {
            ResourceDefinitionData fromRscDfn = ctrlApiDataLoader.loadRscDfn(fromRscNameStr, true);

            SnapshotName fromSnapshotName = LinstorParsingUtils.asSnapshotName(fromSnapshotNameStr);
            SnapshotDefinition fromSnapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(fromRscDfn, fromSnapshotName, true);

            ResourceDefinitionData toRscDfn = ctrlApiDataLoader.loadRscDfn(toRscNameStr, true);

            if (toRscDfn.getResourceCount() != 0)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_RSC,
                    "Cannot restore to resource definition which already has resources"
                ));
            }

            ctrlSnapshotHelper.ensureSnapshotSuccessful(fromSnapshotDfn);

            List<Resource> restoredResources = new ArrayList<>();

            if (nodeNameStrs.isEmpty())
            {
                for (Snapshot snapshot : fromSnapshotDfn.getAllSnapshots(peerAccCtx.get()))
                {
                    restoredResources.add(restoreOnNode(fromSnapshotDfn, toRscDfn, snapshot.getNode()));
                }
            }
            else
            {
                for (String nodeNameStr : nodeNameStrs)
                {
                    NodeData node = ctrlApiDataLoader.loadNode(nodeNameStr, true);
                    restoredResources.add(restoreOnNode(fromSnapshotDfn, toRscDfn, node));
                }
            }

            ctrlTransactionHelper.commit();

            if (toRscDfn.getVolumeDfnCount(peerAccCtx.get()) == 0)
            {
                responseConverter.addWithDetail(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.WARN_NOT_FOUND,
                        "No volumes to restore."
                    )
                    .setDetails("The target resource definition has no volume definitions. " +
                        "The restored resources will be empty.")
                    .setCorrection("Restore the volume definitions to the target resource definition.")
                    .build()
                );
            }

            deploymentResponses = ctrlRscCrtApiHelper.deployResources(context, restoredResources);

            responseConverter.addWithOp(responses, context, ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.CREATED,
                    firstLetterCaps(getSnapshotRestoreDescriptionInline(nodeNameStrs, toRscNameStr)) + " restored " +
                        "from resource '" + fromRscNameStr + "', snapshot '" + fromSnapshotNameStr + "'."
                )
                .setDetails("Resource UUIDs: " +
                    toRscDfn.streamResource(peerAccCtx.get())
                        .map(Resource::getUuid)
                        .map(UUID::toString)
                        .collect(Collectors.joining(", ")))
                .build()
            );

        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.<ApiCallRc>just(responses)
            .concatWith(deploymentResponses)
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty())
            .onErrorResume(EventStreamTimeoutException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeResourceDidNotAppearMessage(context)))
            .onErrorResume(EventStreamClosedException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeEventStreamDisappearedUnexpectedlyMessage(context)));
    }

    private Resource restoreOnNode(SnapshotDefinition fromSnapshotDfn, ResourceDefinitionData toRscDfn, Node node)
        throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        Snapshot snapshot = ctrlApiDataLoader.loadSnapshot(node, fromSnapshotDfn);

        // this will set the layerstack multiple times on the resource definition
        // but it is the easiest way to ensure it is set, should be the same anyway
        toRscDfn.setLayerStack(peerAccCtx.get(), snapshot.getLayerStack(peerAccCtx.get()));

        ResourceData rsc = ctrlRscCrtApiHelper.createResource(
            toRscDfn,
            node,
            snapshot.getNodeId().value,
            0L,
            snapshot.getLayerStack(peerAccCtx.get())
        );

        Iterator<VolumeDefinition> toVlmDfnIter = ctrlRscCrtApiHelper.getVlmDfnIterator(toRscDfn);
        while (toVlmDfnIter.hasNext())
        {
            VolumeDefinition toVlmDfn = toVlmDfnIter.next();
            VolumeNumber volumeNumber = toVlmDfn.getVolumeNumber();

            SnapshotVolumeDefinition fromSnapshotVlmDfn =
                fromSnapshotDfn.getSnapshotVolumeDefinition(peerAccCtx.get(), volumeNumber);

            if (fromSnapshotVlmDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_VLM_DFN,
                    "Snapshot does not contain required volume number " + volumeNumber
                ));
            }

            long snapshotVolumeSize = fromSnapshotVlmDfn.getVolumeSize(peerAccCtx.get());
            long requiredVolumeSize = toVlmDfn.getVolumeSize(peerAccCtx.get());
            if (snapshotVolumeSize != requiredVolumeSize)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_VLM_SIZE,
                    "Snapshot size does not match for volume number " + volumeNumber.value + "; " +
                        "snapshot size: " + snapshotVolumeSize + "KiB, " +
                        "required size: " + requiredVolumeSize + "KiB"
                ));
            }

            SnapshotVolume fromSnapshotVolume = snapshot.getSnapshotVolume(peerAccCtx.get(), volumeNumber);

            if (fromSnapshotVolume == null)
            {
                throw new ImplementationError("Expected snapshot volume missing");
            }

            StorPool storPool = fromSnapshotVolume.getStorPool(peerAccCtx.get());

            Volume vlm = ctrlVlmCrtApiHelper.createVolume(rsc, toVlmDfn, storPool, null, null, null);
            Props vlmProps = vlm.getProps(peerAccCtx.get());
            vlmProps.setProp(
                ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE, fromSnapshotVlmDfn.getResourceName().displayValue);
            vlmProps.setProp(
                ApiConsts.KEY_VLM_RESTORE_FROM_SNAPSHOT, fromSnapshotVlmDfn.getSnapshotName().displayValue);
            String overrideId = fromSnapshotVlmDfn.getProps(
                peerAccCtx.get()).getProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID
            );
            if (overrideId != null)
            {
                vlmProps.setProp(
                    ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID, overrideId);
            }
        }

        return rsc;
    }

    private static String getSnapshotRestoreDescription(List<String> nodeNameStrs, String toRscNameStr)
    {
        return nodeNameStrs.isEmpty() ?
            "Resource: " + toRscNameStr :
            "Nodes: " + String.join(", ", nodeNameStrs) + "; Resource: " + toRscNameStr;
    }

    private static String getSnapshotRestoreDescriptionInline(List<String> nodeNameStrs, String toRscNameStr)
    {
        return nodeNameStrs.isEmpty() ?
            "resource '" + toRscNameStr + "'" :
            "resource '" + toRscNameStr + "' on nodes '" + String.join(", ", nodeNameStrs) + "'";
    }
}
