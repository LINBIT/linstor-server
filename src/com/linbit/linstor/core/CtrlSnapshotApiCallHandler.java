package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotData;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotDefinition.SnapshotDfnFlags;
import com.linbit.linstor.SnapshotDefinitionData;
import com.linbit.linstor.SnapshotDefinitionDataControllerFactory;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
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
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class CtrlSnapshotApiCallHandler extends AbsApiCallHandler
{
    private String currentRscName;
    private String currentSnapshotName;

    private final CtrlClientSerializer clientComSerializer;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ObjectProtection rscDfnMapProt;
    private final SnapshotDefinitionDataControllerFactory snapshotDefinitionDataFactory;
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
    public ApiCallRc createSnapshot(String rscNameStr, String snapshotNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
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

            Iterator<Resource> rscIterator = rscDfn.iterateResource(peerAccCtx);
            while (rscIterator.hasNext())
            {
                Resource rsc = rscIterator.next();

                Snapshot snapshot = new SnapshotData(
                    UUID.randomUUID(),
                    snapshotDfn,
                    rsc.getAssignedNode()
                );
                snapshotDfn.addSnapshot(snapshot);

                rsc.addInProgressSnapshot(snapshotName, snapshot);
            }
            commit();

            updateSatellites(rscDfn);

            eventBroker.openEventStream(EventIdentifier.snapshotDefinition(
                ApiConsts.EVENT_SNAPSHOT_DEPLOYMENT,
                rscDfn.getName(),
                snapshotName
            ));

            reportSuccess(UUID.randomUUID());
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
                getObjectDescriptionInline(rscNameStr, snapshotNameStr),
                getObjRefs(rscNameStr, snapshotNameStr),
                getVariables(rscNameStr, snapshotNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    byte[] listSnapshotDefinitions(int msgId)
    {
        ArrayList<SnapshotDefinition.SnapshotDfnApi> snapshotDfns = new ArrayList<>();
        try
        {
            rscDfnMapProt.requireAccess(peerAccCtx, AccessType.VIEW);
            for (ResourceDefinition rscDfn : rscDfnMap.values())
            {
                for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(peerAccCtx))
                {
                    try
                    {
                        snapshotDfns.add(snapshotDfn.getApiData(peerAccCtx));
                    }
                    catch (AccessDeniedException accDeniedExc)
                    {
                        // don't add storpooldfn without access
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
            ensureSatelliteConnected(currentRsc);
        }
    }

    private void ensureDriversSupportSnapshots(Resource rsc)
        throws AccessDeniedException
    {
        Iterator<Volume> vlmIterator = rsc.iterateVolumes();
        while (vlmIterator.hasNext())
        {
            StorPool storPool = vlmIterator.next().getStorPool(apiCtx);

            if (!storPool.getDriverKind(apiCtx).isSnapshotSupported())
            {
                throw asExc(
                    null,
                    "Storage driver '" + storPool.getDriverName() + "' " + "does not support snapshots.",
                    null, // cause
                    "Used for storage pool '" + storPool.getName() + "' on '" + rsc.getAssignedNode().getName() + "'.",
                    null, // correction
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

    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
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
        currentRscName = rscNameStr;
        currentSnapshotName = snapshotNameStr;
        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Resource: " + currentRscName + ", Snapshot: " + currentSnapshotName;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentRscName, currentSnapshotName);
    }

    private String getObjectDescriptionInline(String rscNameStr, String snapshotNameStr)
    {
        return "snapshot '" + snapshotNameStr + "' of resource '" + rscNameStr + "'";
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
