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
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
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

    public ApiCallRc updateSatellites(Node node)
    {
        return updateSatellites(node, true);
    }

    /**
     * @param node Node to gather info which other nodes are to contact
     * @param contactArgumentNode Flag to indicate if the given node should also be contacted
     */
    public ApiCallRc updateSatellites(Node node, boolean contactArgumentNode)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            Map<NodeName, Node> nodesToContact = new TreeMap<>();
            if (contactArgumentNode)
            {
                nodesToContact.put(node.getName(), node);
            }
            for (Resource rsc : node.streamResources(apiCtx).collect(toList()))
            {
                ResourceDefinition rscDfn = rsc.getDefinition();
                Iterator<Resource> allRscsIterator = rscDfn.iterateResource(apiCtx);
                while (allRscsIterator.hasNext())
                {
                    Resource allRsc = allRscsIterator.next();
                    nodesToContact.put(allRsc.getAssignedNode().getName(), allRsc.getAssignedNode());
                }
            }

            byte[] changedMessage = internalComSerializer
                .onewayBuilder(InternalApiConsts.API_CHANGED_NODE)
                .changedNode(
                    node.getUuid(),
                    node.getName().displayValue
                )
                .build();
            for (Node nodeToContact : nodesToContact.values())
            {
                Peer satellitePeer = nodeToContact.getPeer(apiCtx);
                if (satellitePeer != null)
                {
                    if (satellitePeer.hasFullSyncFailed())
                    {
                        responses.addEntry(makeFullSyncFailedResponse(satellitePeer));
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
                        responses.addEntry(makeFullSyncFailedResponse(currentPeer));
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
                    String nodeName = currentRsc.getAssignedNode().getName().displayValue;
                    responses.addEntry(ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.WARN_NOT_CONNECTED,
                            "No active connection to satellite '" + nodeName + "'"
                        )
                        .setDetails(
                            "The controller is trying to (re-) establish a connection to the satellite. " +
                                "The controller stored the changes and as soon the satellite is connected, it will " +
                                "receive this update."
                        )
                        .build()
                    );
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
                    responses.addEntry(makeFullSyncFailedResponse(satellitePeer));
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
                responses.addEntry(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.WARN_NOT_CONNECTED,
                        "No active connection to satellite '" + node.getName().displayValue + "'"
                    )
                    .setDetails(
                        "The controller is trying to (re-) establish a connection to the satellite. " +
                            "The controller stored the changes and as soon the satellite is connected, it will " +
                            "receive this update."
                    )
                    .build()
                );
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return responses;
    }

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
                        responses.addEntry(makeFullSyncFailedResponse(currentPeer));
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
                    responses.addEntry(ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.WARN_NOT_CONNECTED,
                            "No active connection to satellite '" + snapshot.getNodeName().displayValue + "'"
                        )
                        .setDetails(
                            "The controller is trying to (re-) establish a connection to the satellite. " +
                                "The controller stored the changes and as soon the satellite is connected, it will " +
                                "receive this update."
                        )
                        .build()
                    );
                }
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return responses;
    }

    private static ApiCallRc.RcEntry makeFullSyncFailedResponse(Peer satellite)
    {
        return ApiCallRcImpl
            .entryBuilder(
                ApiConsts.WARN_STLT_NOT_UPDATED,
                "Satellite reported an error during fullSync. This change will NOT be " +
                    "delivered to satellte '" + satellite.getNode().getName().displayValue +
                    "' until the error is resolved. Reconnect the satellite to the controller " +
                    "to remove this blockade."
            )
            .build();
    }
}
