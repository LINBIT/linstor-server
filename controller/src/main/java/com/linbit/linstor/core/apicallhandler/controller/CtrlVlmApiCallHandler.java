package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinitionRepository;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import static com.linbit.linstor.api.ApiConsts.API_LST_VLM;
import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlVlmApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final NodeRepository nodeRepository;
    private final CtrlClientSerializer clientComSerializer;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlVlmApiCallHandler(
        ErrorReporter errorReporterRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        NodeRepository nodeRepositoryRef,
        CtrlClientSerializer clientComSerializerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        nodeRepository = nodeRepositoryRef;
        clientComSerializer = clientComSerializerRef;
        peerAccCtx = peerAccCtxRef;
    }

    byte[] listVolumes(
        long apiCallId,
        List<String> filterNodes,
        List<String> filterStorPools,
        List<String> filterResources
    )
    {
        ArrayList<ResourceData.RscApi> rscs = new ArrayList<>();
        Map<NodeName, SatelliteState> satelliteStates = new HashMap<>();
        try
        {
            final List<String> upperFilterNodes = filterNodes.stream().map(String::toUpperCase).collect(toList());
            final List<String> upperFilterStorPools =
                filterStorPools.stream().map(String::toUpperCase).collect(toList());
            final List<String> upperFilterResources =
                filterResources.stream().map(String::toUpperCase).collect(toList());

            resourceDefinitionRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(rscDfn -> upperFilterResources.isEmpty() ||
                    upperFilterResources.contains(rscDfn.getName().value))
                .forEach(rscDfn ->
                {
                    try
                    {
                        for (Resource rsc : rscDfn.streamResource(peerAccCtx.get())
                            .filter(rsc -> upperFilterNodes.isEmpty() ||
                                upperFilterNodes.contains(rsc.getAssignedNode().getName().value))
                            .collect(toList()))
                        {
                            // create our api object our self to filter the volumes by storage pools

                            // build volume list filtered by storage pools (if provided)
                            List<Volume.VlmApi> volumes = new ArrayList<>();
                            Iterator<Volume> itVolumes = rsc.iterateVolumes();
                            while (itVolumes.hasNext())
                            {
                                Volume vlm = itVolumes.next();
                                if (upperFilterStorPools.isEmpty() ||
                                    upperFilterStorPools.contains(vlm.getStorPool(peerAccCtx.get()).getName().value))
                                {
                                    volumes.add(vlm.getApiData(peerAccCtx.get()));
                                }
                            }

                            List<ResourceConnection.RscConnApi> rscConns = new ArrayList<>();
                            for (ResourceConnection rscConn : rsc.streamResourceConnections(peerAccCtx.get())
                                    .collect(toList()))
                            {
                                rscConns.add(rscConn.getApiData(peerAccCtx.get()));
                            }

                            if (!volumes.isEmpty())
                            {
                                RscPojo filteredRscVlms = new RscPojo(
                                    rscDfn.getName().getDisplayName(),
                                    rsc.getAssignedNode().getName().getDisplayName(),
                                    rsc.getAssignedNode().getUuid(),
                                    rscDfn.getApiData(peerAccCtx.get()),
                                    rsc.getUuid(),
                                    rsc.getStateFlags().getFlagsBits(peerAccCtx.get()),
                                    rsc.getNodeId().value,
                                    rsc.getProps(peerAccCtx.get()).map(),
                                    volumes,
                                    null,
                                    rscConns,
                                    null,
                                    null);
                                rscs.add(filteredRscVlms);
                            }
                        }
                    }
                    catch (AccessDeniedException accDeniedExc)
                    {
                        // don't add rsc without access
                    }
                }
                );

            // get resource states of all nodes
            for (final Node node : nodeRepository.getMapForView(peerAccCtx.get()).values())
            {
                final Peer satellite = node.getPeer(peerAccCtx.get());
                if (satellite != null)
                {
                    Lock readLock = satellite.getSatelliteStateLock().readLock();
                    readLock.lock();
                    try
                    {
                        final SatelliteState satelliteState = satellite.getSatelliteState();

                        if (satelliteState != null)
                        {
                            satelliteStates.put(node.getName(), new SatelliteState(satelliteState));
                        }
                    }
                    finally
                    {
                        readLock.unlock();
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
            errorReporter.reportError(accDeniedExc);
        }

        return clientComSerializer
            .answerBuilder(API_LST_VLM, apiCallId)
            .resourceList(rscs, satelliteStates)
            .build();
    }

    public static String getVlmDescriptionInline(Volume vlm)
    {
        return getVlmDescriptionInline(vlm.getResource(), vlm.getVolumeDefinition());
    }

    public static String getVlmDescriptionInline(Resource rsc, VolumeDefinition vlmDfn)
    {
        return getVlmDescriptionInline(
            rsc.getAssignedNode().getName().displayValue,
            rsc.getDefinition().getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
    }

    public static String getVlmDescriptionInline(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "volume with volume number '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" +
            nodeNameStr + "'";
    }
}
