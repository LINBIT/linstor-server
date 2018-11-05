package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionRepository;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class CtrlSnapshotApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final CtrlClientSerializer clientComSerializer;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlSnapshotApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        CtrlClientSerializer clientComSerializerRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        clientComSerializer = clientComSerializerRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    public ApiCallRc deleteSnapshot(String rscNameStr, String snapshotNameStr)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeDeleteOperation(),
            Collections.emptyList(),
            rscNameStr,
            snapshotNameStr
        );

        try
        {
            ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);

            SnapshotName snapshotName = LinstorParsingUtils.asSnapshotName(snapshotNameStr);
            SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscDfn, snapshotName);

            UUID uuid = snapshotDfn.getUuid();
            if (snapshotDfn.getAllSnapshots(peerAccCtx.get()).isEmpty())
            {
                snapshotDfn.delete(peerAccCtx.get());

                ctrlTransactionHelper.commit();
            }
            else
            {
                markSnapshotDfnDeleted(snapshotDfn);
                for (Snapshot snapshot : snapshotDfn.getAllSnapshots(peerAccCtx.get()))
                {
                    markSnapshotDeleted(snapshot);
                }

                ctrlTransactionHelper.commit();

                responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(snapshotDfn));
            }

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultDeletedEntry(
                uuid, getSnapshotDfnDescriptionInline(rscNameStr, snapshotNameStr)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public void respondSnapshot(long apiCallId, String resourceNameStr, UUID snapshotUuid, String snapshotNameStr)
    {
        try
        {
            ResourceName resourceName = new ResourceName(resourceNameStr);
            SnapshotName snapshotName = new SnapshotName(snapshotNameStr);

            Peer currentPeer = peer.get();

            Snapshot snapshot = null;

            ResourceDefinition rscDefinition = resourceDefinitionRepository.get(apiCtx, resourceName);
            if (rscDefinition != null)
            {
                SnapshotDefinition snapshotDfn = rscDefinition.getSnapshotDfn(apiCtx, snapshotName);
                if (snapshotDfn != null && snapshotDfn.getInProgress(apiCtx))
                {
                    snapshot = snapshotDfn.getSnapshot(peerAccCtx.get(), currentPeer.getNode().getName());
                }
            }

            long fullSyncId = currentPeer.getFullSyncId();
            long updateId = currentPeer.getNextSerializerId();
            if (snapshot != null)
            {
                // TODO: check if the snapshot has the same uuid as snapshotUuid
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_IN_PROGRESS_SNAPSHOT)
                        .snapshotData(snapshot, fullSyncId, updateId)
                        .build()
                );
            }
            else
            {
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_IN_PROGRESS_SNAPSHOT_ENDED)
                        .endedSnapshotData(resourceNameStr, snapshotNameStr, fullSyncId, updateId)
                        .build()
                );
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Satellite requested data for invalid name '" + invalidNameExc.invalidName + "'.",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested storpool data.",
                    accDeniedExc
                )
            );
        }
    }

    byte[] listSnapshotDefinitions(long apiCallId)
    {
        ArrayList<SnapshotDefinition.SnapshotDfnListItemApi> snapshotDfns = new ArrayList<>();
        try
        {
            for (ResourceDefinition rscDfn : resourceDefinitionRepository.getMapForView(peerAccCtx.get()).values())
            {
                for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(peerAccCtx.get()))
                {
                    try
                    {
                        snapshotDfns.add(snapshotDfn.getListItemApiData(peerAccCtx.get()));
                    }
                    catch (AccessDeniedException accDeniedExc)
                    {
                        // don't add snapshot definition without access
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }

        return clientComSerializer
            .answerBuilder(ApiConsts.API_LST_SNAPSHOT_DFN, apiCallId)
            .snapshotDfnList(snapshotDfns)
            .build();
    }

    private void markSnapshotDfnDeleted(SnapshotDefinition snapshotDfn)
    {
        try
        {
            snapshotDfn.markDeleted(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getSnapshotDfnDescriptionInline(snapshotDfn) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void markSnapshotDeleted(Snapshot snapshot)
    {
        try
        {
            snapshot.markDeleted(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getSnapshotDescriptionInline(snapshot) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    public static String getSnapshotDescription(
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr
    )
    {
        String snapshotDescription = "Resource: " + rscNameStr + ", Snapshot: " + snapshotNameStr;
        return nodeNameStrs.isEmpty() ?
            snapshotDescription :
            "Nodes: " + String.join(", ", nodeNameStrs) + "; " + snapshotDescription;
    }

    public static String getSnapshotDescriptionInline(Snapshot snapshot)
    {
        return getSnapshotDescriptionInline(
            Collections.singletonList(snapshot.getNode().getName().displayValue),
            snapshot.getResourceName().displayValue,
            snapshot.getSnapshotName().displayValue
        );
    }

    public static String getSnapshotDescriptionInline(
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr
    )
    {
        String snapshotDescription = getSnapshotDfnDescriptionInline(rscNameStr, snapshotNameStr);
        return nodeNameStrs.isEmpty() ?
            snapshotDescription :
            snapshotDescription + " on nodes '" + String.join(", ", nodeNameStrs) + "'";
    }

    public static String getSnapshotDfnDescriptionInline(SnapshotDefinition snapshotDfn)
    {
        return getSnapshotDfnDescriptionInline(
            snapshotDfn.getResourceName().displayValue, snapshotDfn.getName().displayValue);
    }

    public static String getSnapshotDfnDescriptionInline(
        String rscNameStr,
        String snapshotNameStr
    )
    {
        return "snapshot '" + snapshotNameStr + "' of resource '" + rscNameStr + "'";
    }

    public static String getSnapshotVlmDfnDescriptionInline(SnapshotVolumeDefinition snapshotVlmDfn)
    {
        return getSnapshotVlmDfnDescriptionInline(
            snapshotVlmDfn.getResourceName().displayValue,
            snapshotVlmDfn.getSnapshotName().displayValue,
            snapshotVlmDfn.getVolumeNumber().value
        );
    }

    public static String getSnapshotVlmDfnDescriptionInline(
        String rscNameStr,
        String snapshotNameStr,
        Integer vlmNr
    )
    {
        return "volume definition with number '" + vlmNr +
            "' of snapshot '" + snapshotNameStr + "' of resource '" + rscNameStr + "'";
    }

    public static String getSnapshotVlmDescriptionInline(
        NodeName nodeName,
        ResourceName resourceName,
        SnapshotName snapshotName,
        VolumeNumber volumeNumber
    )
    {
        return "volume with number '" + volumeNumber.value +
            "' of snapshot '" + snapshotName + "' of resource '" + resourceName + "' on '" + nodeName + "'";
    }

    public static ResponseContext makeSnapshotContext(
        ApiOperation operation,
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        objRefs.put(ApiConsts.KEY_SNAPSHOT, snapshotNameStr);

        return new ResponseContext(
            operation,
            getSnapshotDescription(nodeNameStrs, rscNameStr, snapshotNameStr),
            getSnapshotDescriptionInline(nodeNameStrs, rscNameStr, snapshotNameStr),
            ApiConsts.MASK_SNAPSHOT,
            objRefs
        );
    }
}
