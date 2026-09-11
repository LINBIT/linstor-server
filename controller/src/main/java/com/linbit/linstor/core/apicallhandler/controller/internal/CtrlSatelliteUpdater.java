package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import jakarta.inject.Inject;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static java.util.stream.Collectors.toList;

public class CtrlSatelliteUpdater
{
    private final CtrlStltSerializer internalComSerializer;

    @Inject
    private CtrlSatelliteUpdater(
        CtrlStltSerializer serializerRef
    )
    {
        internalComSerializer = serializerRef;
    }

    public static Collection<Node> findNodesToContact(Node node)
    {
        Map<NodeName, Node> nodesToContact = new TreeMap<>();

        if (!node.getNodeType().equals(Node.Type.CONTROLLER))
        {
            nodesToContact.put(node.getName(), node);
        }
        for (Resource rsc : node.streamResources().collect(toList()))
        {
            ResourceDefinition rscDfn = rsc.getResourceDefinition();
            Iterator<Resource> allRscsIterator = rscDfn.iterateResource();
            while (allRscsIterator.hasNext())
            {
                Resource allRsc = allRscsIterator.next();
                nodesToContact.put(allRsc.getNode().getName(), allRsc.getNode());
            }
        }
        return nodesToContact.values();
    }

    public ApiCallRc updateSatellites(Node node)
    {
        return updateSatellites(node.getUuid(), node.getName(), findNodesToContact(node));
    }

    /**
     * Sends node change notifications to the specified satellites synchronously.
     *
     * @param uuid UUID of changed node
     * @param nodeName Name of changed node
     * @param nodesToContact Nodes to update
     */
    public ApiCallRc updateSatellites(UUID uuid, NodeName nodeName, Collection<Node> nodesToContact)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        byte[] changedMessage = internalComSerializer
            .onewayBuilder(InternalApiConsts.API_CHANGED_NODE)
            .changedNode(
                uuid,
                nodeName.displayValue
            )
            .build();
        for (Node nodeToContact : nodesToContact)
        {
            Peer satellitePeer = nodeToContact.getPeer();
            if (satellitePeer.hasFullSyncFailed())
            {
                responses.addEntry(ResponseUtils.makeFullSyncFailedResponse(satellitePeer));
            }
            else if (satellitePeer.isOnline())
            {
                satellitePeer.sendMessage(changedMessage);
            }
        }

        return responses;
    }

    public ApiCallRc updateSatellites(Resource rsc)
    {
        return updateSatellites(rsc.getResourceDefinition());
    }

    public ApiCallRc updateSatellites(ResourceDefinition rscDfn)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        // notify all peers that (at least one of) their resource has changed
        Iterator<Resource> rscIterator = rscDfn.iterateResource();
        while (rscIterator.hasNext())
        {
            Resource currentRsc = rscIterator.next();
            Peer currentPeer = currentRsc.getNode().getPeer();

            boolean connected = currentPeer.isOnline();
            if (connected)
            {
                if (currentPeer.hasFullSyncFailed())
                {
                    responses.addEntry(ResponseUtils.makeFullSyncFailedResponse(currentPeer));
                }
                else
                {
                    currentPeer.sendMessage(
                        internalComSerializer
                            .onewayBuilder(InternalApiConsts.API_CHANGED_RSC)
                            .changedResource(
                                currentRsc.getUuid(),
                                currentRsc.getResourceDefinition().getName().displayValue
                            )
                            .build()
                    );
                }
            }
            else
            {
                responses.addEntry(ResponseUtils.makeNotConnectedWarning(currentRsc.getNode().getName()));
            }
        }

        return responses;
    }

    public ApiCallRc updateSatellite(final StorPool storPool)
    {
        // figure out which nodes to update
        Set<Node> nodesToUpdate = new HashSet<>();
        nodesToUpdate.add(storPool.getNode());

        for (VlmProviderObject<Resource> vlmProviderObject : storPool.getVolumes())
        {
            ResourceDefinition rscDfn = vlmProviderObject.getRscLayerObject()
                .getAbsResource()
                .getResourceDefinition();
            Iterator<Resource> rscIt = rscDfn.iterateResource();
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                nodesToUpdate.add(rsc.getNode());
            }
        }

        return updateSatellite(storPool, nodesToUpdate);
    }

    private ApiCallRc updateSatellite(final StorPool storPoolRef, Set<Node> nodesToUpdateRef)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        final UUID storPoolUuid = storPoolRef.getUuid();
        final String nodeNameStr = storPoolRef.getNode().getName().displayValue;
        final String storPoolNameStr = storPoolRef.getName().displayValue;
        for (Node nodeToUpdate : nodesToUpdateRef)
        {
            Peer peerToUpdate = nodeToUpdate.getPeer();
            boolean connected = peerToUpdate.isOnline();
            if (connected)
            {
                if (peerToUpdate.hasFullSyncFailed())
                {
                    responses.addEntry(ResponseUtils.makeFullSyncFailedResponse(peerToUpdate));
                }
                else
                {
                    peerToUpdate.sendMessage(
                        internalComSerializer
                            .onewayBuilder(InternalApiConsts.API_CHANGED_STOR_POOL)
                            .changedStorPool(
                                storPoolUuid,
                                nodeNameStr,
                                storPoolNameStr
                            )
                            .build()
                    );
                }
            }
            else
            {
                responses.addEntry(ResponseUtils.makeNotConnectedWarning(nodeToUpdate.getName()));
            }
        }

        return responses;
    }
}
