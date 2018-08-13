package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionRepository;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDataControllerFactory;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotDefinition.SnapshotDfnFlags;
import com.linbit.linstor.SnapshotDefinitionData;
import com.linbit.linstor.SnapshotDefinitionDataControllerFactory;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.SnapshotVolumeDataControllerFactory;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.SnapshotVolumeDefinition.SnapshotVlmDfnFlags;
import com.linbit.linstor.SnapshotVolumeDefinitionControllerFactory;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

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

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;

@Singleton
public class CtrlSnapshotApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final SnapshotDefinitionDataControllerFactory snapshotDefinitionDataFactory;
    private final SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactory;
    private final SnapshotDataControllerFactory snapshotDataFactory;
    private final SnapshotVolumeDataControllerFactory snapshotVolumeDataControllerFactory;
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
        SnapshotDefinitionDataControllerFactory snapshotDefinitionDataFactoryRef,
        SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactoryRef,
        SnapshotDataControllerFactory snapshotDataFactoryRef,
        SnapshotVolumeDataControllerFactory snapshotVolumeDataControllerFactoryRef,
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
        snapshotDefinitionDataFactory = snapshotDefinitionDataFactoryRef;
        snapshotVolumeDefinitionControllerFactory = snapshotVolumeDefinitionControllerFactoryRef;
        snapshotDataFactory = snapshotDataFactoryRef;
        snapshotVolumeDataControllerFactory = snapshotVolumeDataControllerFactoryRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        clientComSerializer = clientComSerializerRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    /**
     * Create a snapshot of a resource.
     * <p>
     * Snapshots are created in a multi-stage process:
     * <ol>
     *     <li>Add the snapshot objects (definition and instances), including the in-progress snapshot objects to
     *     be sent to the satellites</li>
     *     <li>When all satellites have received the in-progress snapshots, mark the resource with the suspend flag</li>
     *     <li>When all resources are suspended, send out a snapshot request</li>
     *     <li>When all snapshots have been created, mark the resource as resuming by removing the suspend flag</li>
     *     <li>When all resources have been resumed, remove the in-progress snapshots</li>
     * </ol>
     * This is process is implemented by {@link com.linbit.linstor.event.handler.SnapshotStateMachine}.
     */
    public ApiCallRc createSnapshot(
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeSnapshotContext(
            peer.get(),
            ApiOperation.makeRegisterOperation(),
            nodeNameStrs,
            rscNameStr,
            snapshotNameStr
        );

        try
        {
            ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);

            SnapshotName snapshotName = LinstorParsingUtils.asSnapshotName(snapshotNameStr);
            SnapshotDefinition snapshotDfn = createSnapshotDfnData(
                peerAccCtx.get(),
                rscDfn,
                snapshotName,
                new SnapshotDfnFlags[] {}
            );

            ensureSnapshotsViable(rscDfn);

            rscDfn.addSnapshotDfn(peerAccCtx.get(), snapshotDfn);
            snapshotDfn.setInCreation(peerAccCtx.get(), true);

            Iterator<VolumeDefinition> vlmDfnIterator = rscDfn.iterateVolumeDfn(peerAccCtx.get());
            while (vlmDfnIterator.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIterator.next();

                SnapshotVolumeDefinition snapshotVlmDfn = snapshotVolumeDefinitionControllerFactory.create(
                    apiCtx,
                    snapshotDfn,
                    vlmDfn.getVolumeNumber(),
                    vlmDfn.getVolumeSize(peerAccCtx.get()),
                    new SnapshotVlmDfnFlags[]{}
                );

                boolean isEncrypted = vlmDfn.getFlags().isSet(peerAccCtx.get(), VolumeDefinition.VlmDfnFlags.ENCRYPTED);
                if (isEncrypted)
                {
                    Map<String, String> vlmDfnPropsMap = getVlmDfnProps(vlmDfn).map();
                    Map<String, String> snapshotVlmDfnPropsMaps = getSnapshotVlmDfnProps(snapshotVlmDfn).map();

                    String cryptPasswd = vlmDfnPropsMap.get(ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD);
                    if (cryptPasswd == null)
                    {
                        throw new ImplementationError("Encrypted volume definition without crypt passwd found");
                    }

                    snapshotVlmDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotVlmDfnFlags.ENCRYPTED);
                    snapshotVlmDfnPropsMaps.put(
                        ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD,
                        cryptPasswd
                    );
                }
            }

            if (nodeNameStrs.isEmpty())
            {
                Iterator<Resource> rscIterator = rscDfn.iterateResource(peerAccCtx.get());
                while (rscIterator.hasNext())
                {
                    Resource rsc = rscIterator.next();

                    if (!isDiskless(rsc))
                    {
                        createSnapshotOnNode(snapshotDfn, rsc);
                    }
                }
            }
            else
            {
                for (String nodeNameStr : nodeNameStrs)
                {
                    Resource rsc = rscDfn.getResource(peerAccCtx.get(), LinstorParsingUtils.asNodeName(nodeNameStr));
                    if (rsc == null)
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_NOT_FOUND_RSC,
                            "Resource '" + rscDfn.getName().getDisplayName() +
                                "' on node '" + nodeNameStr + "' not found."
                        ));
                    }

                    if (isDiskless(rsc))
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED,
                            "Cannot create snapshot from diskless resource on node '" + nodeNameStr + "'"
                        ));
                    }
                    createSnapshotOnNode(snapshotDfn, rsc);
                }
            }

            if (snapshotDfn.getAllSnapshots(peerAccCtx.get()).isEmpty())
            {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_NOT_FOUND_RSC,
                            "No resources found for snapshotting"
                        ));
                }

            ctrlTransactionHelper.commit();

            responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(snapshotDfn));

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultRegisteredEntry(
                snapshotDfn.getUuid(), getSnapshotDescriptionInline(nodeNameStrs, rscNameStr, snapshotNameStr)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private void createSnapshotOnNode(SnapshotDefinition snapshotDfn, Resource rsc)
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        Snapshot snapshot = snapshotDataFactory.create(
            apiCtx,
            rsc.getAssignedNode(),
            snapshotDfn,
            new Snapshot.SnapshotFlags[]{}
        );

        for (SnapshotVolumeDefinition snapshotVolumeDefinition :
            snapshotDfn.getAllSnapshotVolumeDefinitions(peerAccCtx.get()))
        {
            snapshotVolumeDataControllerFactory.create(
                apiCtx,
                snapshot,
                snapshotVolumeDefinition,
                rsc.getVolume(snapshotVolumeDefinition.getVolumeNumber()).getStorPool(apiCtx)
            );
        }
    }

    public ApiCallRc deleteSnapshot(String rscNameStr, String snapshotNameStr)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeSnapshotContext(
            peer.get(),
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

    private void ensureSnapshotsViable(ResourceDefinitionData rscDfn)
        throws AccessDeniedException
    {
        Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
        while (rscIterator.hasNext())
        {
            Resource currentRsc = rscIterator.next();
            ensureDriversSupportSnapshots(currentRsc);
            ensureInternalMetaDisks(currentRsc);
            ensureSatelliteConnected(currentRsc);
        }
    }

    private void ensureDriversSupportSnapshots(Resource rsc)
        throws AccessDeniedException
    {
        if (!isDiskless(rsc))
        {
            Iterator<Volume> vlmIterator = rsc.iterateVolumes();
            while (vlmIterator.hasNext())
            {
                StorPool storPool = vlmIterator.next().getStorPool(apiCtx);

                if (!storPool.getDriverKind().isSnapshotSupported())
                {
                    throw new ApiRcException(ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED,
                            "Storage driver '" + storPool.getDriverName() + "' " + "does not support snapshots."
                        )
                        .setDetails("Used for storage pool '" + storPool.getName() + "'" +
                            " on '" + rsc.getAssignedNode().getName() + "'.")
                        .build()
                    );
                }
            }
        }
    }

    private void ensureInternalMetaDisks(Resource rsc)
        throws AccessDeniedException
    {
        Iterator<Volume> vlmIterator = rsc.iterateVolumes();
        while (vlmIterator.hasNext())
        {
            Volume vlm = vlmIterator.next();

            String metaDiskPath = vlm.getMetaDiskPath(peerAccCtx.get());
            if (metaDiskPath != null && !metaDiskPath.isEmpty() && !metaDiskPath.equals("internal"))
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED,
                        "Snapshot with external meta-disk not supported."
                    )
                    .setDetails("Volume " + vlm.getVolumeDefinition().getVolumeNumber().value +
                        " on node " + rsc.getAssignedNode().getName().displayValue +
                        " has meta disk path '" + metaDiskPath + "'")
                    .build()
                );
            }
        }
    }

    private void ensureSatelliteConnected(Resource rsc)
        throws AccessDeniedException
    {
        Node node = rsc.getAssignedNode();
        Peer currentPeer = node.getPeer(apiCtx);

        boolean connected = currentPeer.isConnected();
        if (!connected)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_CONNECTED,
                    "No active connection to satellite '" + node.getName() + "'."
                )
                .setDetails("Snapshots cannot be created when the corresponding satellites are not connected.")
                .build()
            );
        }
    }

    private boolean isDiskless(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            isDiskless = rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISKLESS);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }

    private SnapshotDefinitionData createSnapshotDfnData(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        SnapshotDfnFlags[] snapshotDfnInitFlags
    )
    {
        SnapshotDefinitionData snapshotDfn;
        try
        {
            snapshotDfn = snapshotDefinitionDataFactory.create(
                accCtx,
                rscDfn,
                snapshotName,
                snapshotDfnInitFlags
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getSnapshotDfnDescriptionInline(rscDfn.getName().displayValue, snapshotName.displayValue),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN, String.format(
                "A snapshot definition with the name '%s' already exists in resource definition '%s'.",
                snapshotName,
                rscDfn.getName().displayValue
            )), dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return snapshotDfn;
    }

    private Props getVlmDfnProps(VolumeDefinition vlmDfn)
    {
        Props props;
        try
        {
            props = vlmDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the properties of " + getVlmDfnDescriptionInline(vlmDfn),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return props;
    }

    private Props getSnapshotVlmDfnProps(SnapshotVolumeDefinition snapshotVlmDfn)
    {
        Props props;
        try
        {
            props = snapshotVlmDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the properties of " + getSnapshotVlmDfnDescriptionInline(snapshotVlmDfn),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_VLM_DFN
            );
        }
        return props;
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

    private static ResponseContext makeSnapshotContext(
        Peer peer,
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
            peer,
            operation,
            getSnapshotDescription(nodeNameStrs, rscNameStr, snapshotNameStr),
            getSnapshotDescriptionInline(nodeNameStrs, rscNameStr, snapshotNameStr),
            ApiConsts.MASK_SNAPSHOT,
            objRefs
        );
    }
}
