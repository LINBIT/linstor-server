package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
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
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class CtrlSnapshotApiCallHandler extends AbsApiCallHandler
{
    private List<String> currentNodeNames;
    private String currentRscName;
    private String currentSnapshotName;

    private final CtrlClientSerializer clientComSerializer;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ObjectProtection rscDfnMapProt;
    private final SnapshotDefinitionDataControllerFactory snapshotDefinitionDataFactory;
    private final SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactory;
    private final SnapshotDataControllerFactory snapshotDataFactory;
    private final SnapshotVolumeDataControllerFactory snapshotVolumeDataControllerFactory;
    private final EventBroker eventBroker;

    @Inject
    public CtrlSnapshotApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        CtrlClientSerializer clientComSerializerRef,
        @ApiContext AccessContext apiCtxRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef,
        CtrlObjectFactories objectFactories,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef,
        SnapshotDefinitionDataControllerFactory snapshotDefinitionDataControllerFactoryRef,
        SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactoryRef,
        SnapshotDataControllerFactory snapshotDataFactoryRef,
        SnapshotVolumeDataControllerFactory snapshotVolumeDataControllerFactoryRef,
        EventBroker eventBrokerRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            LinStorObject.SNAPSHOT,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        clientComSerializer = clientComSerializerRef;
        rscDfnMap = rscDfnMapRef;
        rscDfnMapProt = rscDfnMapProtRef;
        snapshotDefinitionDataFactory = snapshotDefinitionDataControllerFactoryRef;
        snapshotVolumeDefinitionControllerFactory = snapshotVolumeDefinitionControllerFactoryRef;
        snapshotDataFactory = snapshotDataFactoryRef;
        snapshotVolumeDataControllerFactory = snapshotVolumeDataControllerFactoryRef;
        eventBroker = eventBrokerRef;
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
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
                nodeNameStrs,
                rscNameStr,
                snapshotNameStr
            )
        )
        {
            ResourceDefinitionData rscDfn = loadRscDfn(rscNameStr, true);

            SnapshotName snapshotName = asSnapshotName(snapshotNameStr);
            SnapshotDefinition snapshotDfn = createSnapshotDfnData(
                peerAccCtx,
                rscDfn,
                snapshotName,
                new SnapshotDfnFlags[] {}
            );

            ensureSnapshotsViable(rscDfn);

            rscDfn.addSnapshotDfn(peerAccCtx, snapshotDfn);
            snapshotDfn.setInCreation(peerAccCtx, true);

            Iterator<VolumeDefinition> vlmDfnIterator = rscDfn.iterateVolumeDfn(peerAccCtx);
            while (vlmDfnIterator.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIterator.next();

                SnapshotVolumeDefinition snapshotVlmDfn = snapshotVolumeDefinitionControllerFactory.getInstance(
                    apiCtx,
                    snapshotDfn,
                    vlmDfn.getVolumeNumber(),
                    vlmDfn.getVolumeSize(peerAccCtx),
                    new SnapshotVlmDfnFlags[]{},
                    true,
                    true
                );

                boolean isEncrypted = vlmDfn.getFlags().isSet(peerAccCtx, VolumeDefinition.VlmDfnFlags.ENCRYPTED);
                if (isEncrypted)
                {
                    Map<String, String> vlmDfnPropsMap = getVlmDfnProps(vlmDfn).map();
                    Map<String, String> snapshotVlmDfnPropsMaps = getSnapshotVlmDfnProps(snapshotVlmDfn).map();

                    String cryptPasswd = vlmDfnPropsMap.get(ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD);
                    if (cryptPasswd == null)
                    {
                        throw new ImplementationError("Encrypted volume definition without crypt passwd found");
                    }

                    snapshotVlmDfn.getFlags().enableFlags(peerAccCtx, SnapshotVlmDfnFlags.ENCRYPTED);
                    snapshotVlmDfnPropsMaps.put(
                        ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD,
                        cryptPasswd
                    );
                }
            }

            if (nodeNameStrs.isEmpty())
            {
                Iterator<Resource> rscIterator = rscDfn.iterateResource(peerAccCtx);
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
                    Resource rsc = rscDfn.getResource(peerAccCtx, asNodeName(nodeNameStr));
                    if (rsc == null)
                    {
                        throw asExc(
                            null,
                            "Resource '" + rscDfn.getName().getDisplayName() +
                                "' on node '" + nodeNameStr + "' not found.",
                            ApiConsts.FAIL_NOT_FOUND_RSC
                        );
                    }

                    if (isDiskless(rsc))
                    {
                        throw asExc(
                            null,
                            "Cannot create snapshot from diskless resource on node '" + nodeNameStr + "'",
                            ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED
                        );
                    }
                    createSnapshotOnNode(snapshotDfn, rsc);
                }
            }

            if (snapshotDfn.getAllSnapshots(peerAccCtx).isEmpty())
            {
                throw asExc(
                    null,
                    "No resources found for snapshotting",
                    ApiConsts.FAIL_NOT_FOUND_RSC
                );
            }

            commit();

            updateSatellites(snapshotDfn);

            eventBroker.openEventStream(EventIdentifier.snapshotDefinition(
                ApiConsts.EVENT_SNAPSHOT_DEPLOYMENT,
                rscDfn.getName(),
                snapshotName
            ));

            reportSuccess(snapshotDfn.getUuid());
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(nodeNameStrs, rscNameStr, snapshotNameStr),
                getObjRefs(rscNameStr, snapshotNameStr),
                getVariables(rscNameStr, snapshotNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    private void createSnapshotOnNode(SnapshotDefinition snapshotDfn, Resource rsc)
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        Snapshot snapshot = snapshotDataFactory.getInstance(
            apiCtx,
            rsc.getAssignedNode(),
            snapshotDfn,
            new Snapshot.SnapshotFlags[]{},
            true,
            true
        );

        for (SnapshotVolumeDefinition snapshotVolumeDefinition :
            snapshotDfn.getAllSnapshotVolumeDefinitions(peerAccCtx))
        {
            snapshotVolumeDataControllerFactory.getInstance(
                apiCtx,
                snapshot,
                snapshotVolumeDefinition,
                rsc.getVolume(snapshotVolumeDefinition.getVolumeNumber()).getStorPool(apiCtx),
                true,
                true
            );
        }
    }

    public ApiCallRc deleteSnapshot(String rscNameStr, String snapshotNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                Collections.emptyList(),
                rscNameStr,
                snapshotNameStr
            )
        )
        {
            ResourceDefinitionData rscDfn = loadRscDfn(rscNameStr, true);

            SnapshotName snapshotName = asSnapshotName(snapshotNameStr);
            SnapshotDefinition snapshotDfn = loadSnapshotDfn(rscDfn, snapshotName);

            UUID uuid = snapshotDfn.getUuid();
            if (snapshotDfn.getAllSnapshots(peerAccCtx).isEmpty())
            {
                snapshotDfn.delete(peerAccCtx);

                commit();
            }
            else
            {
                snapshotDfn.markDeleted(peerAccCtx);
                for (Snapshot snapshot : snapshotDfn.getAllSnapshots(peerAccCtx))
                {
                    snapshot.markDeleted(peerAccCtx);
                }

                commit();

                updateSatellites(snapshotDfn);
            }

            reportSuccess(uuid);
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.DELETE,
                getObjectDescriptionInline(Collections.emptyList(), rscNameStr, snapshotNameStr),
                getObjRefs(rscNameStr, snapshotNameStr),
                getVariables(rscNameStr, snapshotNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    public void respondSnapshot(int msgId, String resourceNameStr, UUID snapshotUuid, String snapshotNameStr)
    {
        try
        {
            ResourceName resourceName = new ResourceName(resourceNameStr);
            SnapshotName snapshotName = new SnapshotName(snapshotNameStr);

            Peer currentPeer = peer.get();

            Snapshot snapshot = null;

            ResourceDefinition rscDefinition = rscDfnMap.get(resourceName);
            if (rscDefinition != null)
            {
                SnapshotDefinition snapshotDfn = rscDefinition.getSnapshotDfn(apiCtx, snapshotName);
                if (snapshotDfn != null && snapshotDfn.getInProgress(apiCtx))
                {
                    snapshot = snapshotDfn.getSnapshot(peerAccCtx, currentPeer.getNode().getName());
                }
            }

            long fullSyncId = currentPeer.getFullSyncId();
            long updateId = currentPeer.getNextSerializerId();
            if (snapshot != null)
            {
                // TODO: check if the snapshot has the same uuid as snapshotUuid
                currentPeer.sendMessage(
                    internalComSerializer
                        .builder(InternalApiConsts.API_APPLY_IN_PROGRESS_SNAPSHOT, msgId)
                        .snapshotData(snapshot, fullSyncId, updateId)
                        .build()
                );
            }
            else
            {
                currentPeer.sendMessage(
                    internalComSerializer
                        .builder(InternalApiConsts.API_APPLY_IN_PROGRESS_SNAPSHOT_ENDED, msgId)
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

    byte[] listSnapshotDefinitions(int msgId)
    {
        ArrayList<SnapshotDefinition.SnapshotDfnListItemApi> snapshotDfns = new ArrayList<>();
        try
        {
            rscDfnMapProt.requireAccess(peerAccCtx, AccessType.VIEW);
            for (ResourceDefinition rscDfn : rscDfnMap.values())
            {
                for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(peerAccCtx))
                {
                    try
                    {
                        snapshotDfns.add(snapshotDfn.getListItemApiData(peerAccCtx));
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
            .builder(ApiConsts.API_LST_SNAPSHOT_DFN, msgId)
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
                    throw asExc(
                        null,
                        "Storage driver '" + storPool.getDriverName() + "' " + "does not support snapshots.",
                        null, // cause
                        "Used for storage pool '" + storPool.getName() + "'" +
                            " on '" + rsc.getAssignedNode().getName() + "'.",
                        null, // correction
                        ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED
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

            String metaDiskPath = vlm.getMetaDiskPath(peerAccCtx);
            if (metaDiskPath != null && !metaDiskPath.isEmpty() && !metaDiskPath.equals("internal"))
            {
                throw asExc(
                    null,
                    "Snapshot with external meta-disk not supported.",
                    null,
                    "Volume " + vlm.getVolumeDefinition().getVolumeNumber().value +
                        " on node " + rsc.getAssignedNode().getName().displayValue +
                        " has meta disk path '" + metaDiskPath + "'",
                    null,
                    ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED
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
            throw asExc(
                null,
                "No active connection to satellite '" + node.getName() + "'.",
                null, // cause
                "Snapshots cannot be created when the corresponding satellites are not connected.",
                null, // correction
                ApiConsts.FAIL_NOT_CONNECTED
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
            throw asImplError(implError);
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
            snapshotDfn = snapshotDefinitionDataFactory.getInstance(
                accCtx,
                rscDfn,
                snapshotName,
                snapshotDfnInitFlags,
                true,
                true
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "create " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw asExc(
                dataAlreadyExistsExc,
                String.format(
                    "A snapshot definition with the name '%s' already exists in resource definition '%s'.",
                    snapshotName,
                    currentRscName
                ),
                ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "creating " + getObjectDescriptionInline()
            );
        }
        return snapshotDfn;
    }

    private Props getVlmDfnProps(VolumeDefinition vlmDfn)
    {
        Props props;
        try
        {
            props = vlmDfn.getProps(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access the properties of " + getObjectDescriptionInline(),
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
            props = snapshotVlmDfn.getProps(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access the properties of " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_VLM_DFN
            );
        }
        return props;
    }

    private void markSnapshotDfnDeleted(SnapshotDefinition snapshotDfn)
    {
        try
        {
            snapshotDfn.markDeleted(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "mark " + getObjectDescriptionInline() + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + getObjectDescriptionInline()
            );
        }
    }

    private void markSnapshotDeleted(Snapshot snapshot)
    {
        try
        {
            snapshot.markDeleted(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "mark " + getObjectDescriptionInline() + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + getObjectDescriptionInline()
            );
        }
    }

    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr
    )
    {
        super.setContext(
            type,
            apiCallRc,
            true,
            getObjRefs(rscNameStr, snapshotNameStr),
            getVariables(rscNameStr, snapshotNameStr)
        );
        currentNodeNames = nodeNameStrs;
        currentRscName = rscNameStr;
        currentSnapshotName = snapshotNameStr;
        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        String snapshotDescription = "Resource: " + currentRscName + ", Snapshot: " + currentSnapshotName;
        return currentNodeNames.isEmpty() ?
            snapshotDescription :
            "Nodes: " + String.join(", ", currentNodeNames) + "; " + snapshotDescription;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentNodeNames, currentRscName, currentSnapshotName);
    }

    private String getObjectDescriptionInline(
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr
    )
    {
        String snapshotDescription = "snapshot '" + snapshotNameStr + "' of resource '" + rscNameStr + "'";
        return nodeNameStrs.isEmpty() ?
            snapshotDescription :
            snapshotDescription + " on nodes '" + String.join(", ", nodeNameStrs) + "'";
    }

    private Map<String, String> getObjRefs(String rscNameStr, String snapshotNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        map.put(ApiConsts.KEY_SNAPSHOT, snapshotNameStr);
        return map;
    }

    private Map<String, String> getVariables(String rscNameStr, String snapshotNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_RSC_NAME, rscNameStr);
        map.put(ApiConsts.KEY_SNAPSHOT_NAME, snapshotNameStr);
        return map;
    }
}
