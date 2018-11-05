package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static java.util.stream.Collectors.toList;

public class CtrlSatelliteUpdater
{
    private final AccessContext apiCtx;
    private final CtrlStltSerializer internalComSerializer;

    @Inject
    private CtrlSatelliteUpdater(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlStltSerializer serializerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef
    )
    {
        apiCtx = apiCtxRef;
        internalComSerializer = serializerRef;
    }

    public static Collection<Node> findNodesToContact(AccessContext accCtx, Node node)
    {
        Map<NodeName, Node> nodesToContact = new TreeMap<>();

        try
        {
            nodesToContact.put(node.getName(), node);
            for (Resource rsc : node.streamResources(accCtx).collect(toList()))
            {
                ResourceDefinition rscDfn = rsc.getDefinition();
                Iterator<Resource> allRscsIterator = rscDfn.iterateResource(accCtx);
                while (allRscsIterator.hasNext())
                {
                    Resource allRsc = allRscsIterator.next();
                    nodesToContact.put(allRsc.getAssignedNode().getName(), allRsc.getAssignedNode());
                }
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return nodesToContact.values();
    }

    public ApiCallRc updateSatellites(Node node)
    {
        return updateSatellites(node.getUuid(), node.getName(), findNodesToContact(apiCtx, node));
    }

    /**
     * @param uuid UUID of changed node
     * @param nodeName Name of changed node
     * @param nodesToContact Nodes to update
     */
    public ApiCallRc updateSatellites(UUID uuid, NodeName nodeName, Collection<Node> nodesToContact)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            byte[] changedMessage = internalComSerializer
                .onewayBuilder(InternalApiConsts.API_CHANGED_NODE)
                .changedNode(
                    uuid,
                    nodeName.displayValue
                )
                .build();
            for (Node nodeToContact : nodesToContact)
            {
                Peer satellitePeer = nodeToContact.getPeer(apiCtx);
                if (satellitePeer != null)
                {
                    if (satellitePeer.hasFullSyncFailed())
                    {
                        responses.addEntry(ResponseUtils.makeFullSyncFailedResponse(satellitePeer));
                    }
                    else if (satellitePeer.isConnected())
                    {
                        satellitePeer.sendMessage(changedMessage);
                    }
                }
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return responses;
    }

    public ApiCallRc updateSatellites(Resource rsc)
    {
        return updateSatellites(rsc.getDefinition());
    }

    public ApiCallRc updateSatellites(ResourceDefinition rscDfn)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            // notify all peers that (at least one of) their resource has changed
            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            while (rscIterator.hasNext())
            {
                Resource currentRsc = rscIterator.next();
                Peer currentPeer = currentRsc.getAssignedNode().getPeer(apiCtx);

                boolean connected = currentPeer.isConnected();
                if (connected)
                {
                    if (currentPeer.hasFullSyncFailed())
                    {
                        responses.addEntry(ResponseUtils.makeFullSyncFailedResponse(currentPeer));
                    }
                    else
                    {
                        connected = currentPeer.sendMessage(
                            internalComSerializer
                                .onewayBuilder(InternalApiConsts.API_CHANGED_RSC)
                                .changedResource(
                                    currentRsc.getUuid(),
                                    currentRsc.getDefinition().getName().displayValue
                                )
                                .build()
                        );
                    }
                }
                else
                {
                    responses.addEntry(ResponseUtils.makeNotConnectedWarning(currentRsc.getAssignedNode().getName()));
                }
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return responses;
    }

    public ApiCallRc updateSatellite(final StorPool storPool)
    {
        return updateSatellite(storPool.getNode(), storPool.getName(), storPool.getUuid());
    }

    public ApiCallRc updateSatellite(final Node node, final StorPoolName storPoolName, final UUID storPoolUuid)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            Peer satellitePeer = node.getPeer(apiCtx);
            boolean connected = satellitePeer.isConnected();
            if (connected)
            {
                if (satellitePeer.hasFullSyncFailed())
                {
                    responses.addEntry(ResponseUtils.makeFullSyncFailedResponse(satellitePeer));
                }
                else
                {
                    connected = satellitePeer.sendMessage(
                        internalComSerializer
                            .onewayBuilder(InternalApiConsts.API_CHANGED_STOR_POOL)
                            .changedStorPool(
                                storPoolUuid,
                                storPoolName.displayValue
                            )
                            .build()
                    );
                }
            }
            else
            {
                responses.addEntry(ResponseUtils.makeNotConnectedWarning(node.getName()));
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return responses;
    }

    // TODO remove
    public ApiCallRc updateSatellites(SnapshotDefinition snapshotDfn)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            // notify all peers that a snapshot has changed
            for (Snapshot snapshot : snapshotDfn.getAllSnapshots(apiCtx))
            {
                Peer currentPeer = snapshot.getNode().getPeer(apiCtx);

                boolean connected = currentPeer.isConnected();
                if (connected)
                {
                    if (currentPeer.hasFullSyncFailed())
                    {
                        responses.addEntry(ResponseUtils.makeFullSyncFailedResponse(currentPeer));
                    }
                    else
                    {
                        connected = currentPeer.sendMessage(
                            internalComSerializer
                                .onewayBuilder(InternalApiConsts.API_CHANGED_IN_PROGRESS_SNAPSHOT)
                                .changedSnapshot(
                                    snapshotDfn.getResourceName().displayValue,
                                    snapshot.getUuid(),
                                    snapshot.getSnapshotName().displayValue
                                )
                                .build()
                        );
                    }
                }
                if (!connected)
                {
                    responses.addEntry(ResponseUtils.makeNotConnectedWarning(snapshot.getNodeName()));
                }
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return responses;
    }
}
