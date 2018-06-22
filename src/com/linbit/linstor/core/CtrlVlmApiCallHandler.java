package com.linbit.linstor.core;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import com.linbit.ImplementationError;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;

import static com.linbit.linstor.api.ApiConsts.API_LST_VLM;
import static java.util.stream.Collectors.toList;

public class CtrlVlmApiCallHandler extends AbsApiCallHandler
{
    private String currentNodeName;
    private String currentRscName;
    private Integer currentVlmNr;
    private final CtrlClientSerializer clientComSerializer;
    private final ObjectProtection rscDfnMapProt;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ObjectProtection nodesMapProt;
    private final CoreModule.NodesMap nodesMap;

    @Inject
    protected CtrlVlmApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        CtrlClientSerializer clientComSerializerRef,
        @ApiContext AccessContext apiCtxRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Named(ControllerSecurityModule.NODES_MAP_PROT) ObjectProtection nodesMapProtRef,
        CoreModule.NodesMap nodesMapRef,
        CtrlObjectFactories objectFactories,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            LinStorObject.VOLUME,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        clientComSerializer = clientComSerializerRef;
        rscDfnMapProt = rscDfnMapProtRef;
        rscDfnMap = rscDfnMapRef;
        nodesMapProt = nodesMapProtRef;
        nodesMap = nodesMapRef;
    }

    ApiCallRc volumeResized(
        String nodeNameStr,
        String rscNameStr,
        int volumeNr,
        long vlmSize
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.MODIFY,
                apiCallRc,
                nodeNameStr,
                rscNameStr,
                volumeNr
            )
        )
        {
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr, true);
            VolumeNumber volumeNumber = asVlmNr(volumeNr);

            Volume vlm = rscData.getVolume(volumeNumber);

            boolean updateSatellites = false;

            boolean resizeExpected = vlm.getFlags().isSet(apiCtx, VlmFlags.RESIZE);

            if (resizeExpected)
            {
                // Verify that this resize matches the current target size
                long expectedSize = vlm.getVolumeDefinition().getVolumeSize(apiCtx);
                if (vlmSize == expectedSize)
                {
                    errorReporter.logDebug("Volume %s resized to %d.", vlm, vlmSize);

                    vlm.getFlags().disableFlags(apiCtx, VlmFlags.RESIZE);

                    // If all volumes have been resized, can resize DRBD
                    boolean allResized = true;
                    Iterator<Volume> vlmIter = vlm.getVolumeDefinition().iterateVolumes(apiCtx);
                    while (vlmIter.hasNext())
                    {
                        Volume otherVlm = vlmIter.next();

                        if (otherVlm.getFlags().isSet(apiCtx, VlmFlags.RESIZE))
                        {
                            allResized = false;
                            break;
                        }
                    }

                    if (allResized)
                    {
                        vlm.getFlags().enableFlags(apiCtx, VlmFlags.DRBD_RESIZE);
                        updateSatellites = true;
                    }
                }
                else
                {
                    errorReporter.logDebug(
                        "Volume %s resized to %d, awaiting resize to %d.", vlm, vlmSize, expectedSize);
                }
            }

            commit();

            if (updateSatellites)
            {
                updateSatellites(rscData);
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.MODIFY,
                getObjectDescriptionInline(nodeNameStr, rscNameStr, volumeNr),
                getObjRefs(nodeNameStr, rscNameStr, volumeNr),
                getVariables(nodeNameStr, rscNameStr, volumeNr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    ApiCallRc volumeDrbdResized(
        String nodeNameStr,
        String rscNameStr,
        int volumeNr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.MODIFY,
                apiCallRc,
                nodeNameStr,
                rscNameStr,
                volumeNr
            )
        )
        {
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr, true);
            VolumeNumber volumeNumber = asVlmNr(volumeNr);

            Volume vlm = rscData.getVolume(volumeNumber);

            vlm.getFlags().disableFlags(apiCtx, VlmFlags.DRBD_RESIZE);

            commit();
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.MODIFY,
                getObjectDescriptionInline(nodeNameStr, rscNameStr, volumeNr),
                getObjRefs(nodeNameStr, rscNameStr, volumeNr),
                getVariables(nodeNameStr, rscNameStr, volumeNr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    ApiCallRc volumeDeleted(
        String nodeNameStr,
        String rscNameStr,
        int volumeNr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                nodeNameStr,
                rscNameStr,
                volumeNr
            )
        )
        {
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr, true);
            VolumeNumber volumeNumber = asVlmNr(volumeNr);

            Volume vlm = rscData.getVolume(volumeNumber);
            UUID vlmUuid = vlm.getUuid(); // prevent access to deleted object

            markClean(vlm);

            boolean allVlmsClean = true;
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
            UUID vlmDfnUuid = vlmDfn.getUuid();
            Iterator<Volume> vlmIterator = getVolumeIterator(vlmDfn);
            while (vlmIterator.hasNext())
            {
                Volume volume = vlmIterator.next();
                if (!isMarkedAsClean(volume))
                {
                    allVlmsClean = false;
                    break;
                }
            }
            boolean vlmDfnDeleted = false;
            if (allVlmsClean && isMarkedForDeletion(vlmDfn))
            {
                delete(vlmDfn); // also deletes all of its volumes
                vlmDfnDeleted = true;
            }

            commit();

            reportSuccess(vlmUuid);
            errorReporter.logDebug(
                String.format("Volume with number '%d' deleted on node '%s'.", volumeNr, nodeNameStr)
            );
            if (vlmDfnDeleted)
            {
                reportSuccess(
                    "Volume definition with number '" + volumeNumber.value + "' of resource definition '" +
                        rscData.getDefinition().getName().displayValue + "' deleted.",
                        "Volume definition's UUID was: " + vlmDfnUuid.toString(),
                        ApiConsts.MASK_DEL | ApiConsts.MASK_VLM_DFN | ApiConsts.DELETED
                );
            }
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
                getObjectDescriptionInline(nodeNameStr, rscNameStr, volumeNr),
                getObjRefs(nodeNameStr, rscNameStr, volumeNr),
                getVariables(nodeNameStr, rscNameStr, volumeNr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    byte[] listVolumes(
        int msgId,
        List<String> filterNodes,
        List<String> filterStorPools,
        List<String> filterResources
    )
    {
        ArrayList<ResourceData.RscApi> rscs = new ArrayList<>();
        Map<NodeName, SatelliteState> satelliteStates = new HashMap<>();
        try
        {
            rscDfnMapProt.requireAccess(peerAccCtx, AccessType.VIEW);
            nodesMapProt.requireAccess(peerAccCtx, AccessType.VIEW);

            final List<String> upperFilterNodes = filterNodes.stream().map(String::toUpperCase).collect(toList());
            final List<String> upperFilterStorPools =
                filterStorPools.stream().map(String::toUpperCase).collect(toList());
            final List<String> upperFilterResources =
                filterResources.stream().map(String::toUpperCase).collect(toList());

            rscDfnMap.values().stream()
                .filter(rscDfn -> upperFilterResources.isEmpty() ||
                    upperFilterResources.contains(rscDfn.getName().value))
                .forEach(rscDfn ->
                {
                    try
                    {
                        for (Resource rsc : rscDfn.streamResource(peerAccCtx)
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
                                    upperFilterStorPools.contains(vlm.getStorPool(peerAccCtx).getName().value))
                                {
                                    volumes.add(vlm.getApiData(peerAccCtx));
                                }
                            }

                            List<ResourceConnection.RscConnApi> rscConns = new ArrayList<>();
                            for (ResourceConnection rscConn : rsc.streamResourceConnections(peerAccCtx)
                                    .collect(toList()))
                            {
                                rscConns.add(rscConn.getApiData(peerAccCtx));
                            }

                            if (!volumes.isEmpty())
                            {
                                RscPojo filteredRscVlms = new RscPojo(
                                    rscDfn.getName().getDisplayName(),
                                    rsc.getAssignedNode().getName().getDisplayName(),
                                    rsc.getAssignedNode().getUuid(),
                                    rscDfn.getApiData(peerAccCtx),
                                    rsc.getUuid(),
                                    rsc.getStateFlags().getFlagsBits(peerAccCtx),
                                    rsc.getNodeId().value,
                                    rsc.getProps(peerAccCtx).map(),
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
            for (final Node node : nodesMap.values())
            {
                final Peer peer = node.getPeer(peerAccCtx);
                if (peer != null)
                {
                    Lock readLock = peer.getSatelliteStateLock().readLock();
                    readLock.lock();
                    try
                    {
                        final SatelliteState satelliteState = peer.getSatelliteState();

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
            .builder(API_LST_VLM, msgId)
            .resourceList(rscs, satelliteStates)
            .build();
    }

    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        String nodeNameStr,
        String rscNameStr,
        Integer vlmNr
    )
    {
        super.setContext(
            type,
            apiCallRc,
            true, // autoClose
            getObjRefs(nodeNameStr, rscNameStr, vlmNr),
            getVariables(nodeNameStr, rscNameStr, vlmNr)
        );
        currentNodeName = nodeNameStr;
        currentRscName = rscNameStr;
        currentVlmNr = vlmNr;

        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Node: " + currentNodeName + ", Resource: " + currentRscName +
            " Volume number: " + currentVlmNr;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentNodeName, currentRscName, currentVlmNr);
    }

    private String getObjectDescriptionInline(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "volume with volume number '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" +
            nodeNameStr + "'";
    }

    private Map<String, String> getObjRefs(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE, nodeNameStr);
        map.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        if (vlmNr != null)
        {
            map.put(ApiConsts.KEY_VLM_NR, rscNameStr);
        }
        return map;
    }

    private Map<String, String> getVariables(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE_NAME, nodeNameStr);
        map.put(ApiConsts.KEY_RSC_NAME, rscNameStr);
        if (vlmNr != null)
        {
            map.put(ApiConsts.KEY_VLM_NR, rscNameStr);
        }
        return map;
    }

    private void markClean(Volume vol)
    {
        try
        {
            vol.getFlags().enableFlags(apiCtx, VlmFlags.CLEAN);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "marking " + getObjectDescriptionInline() + " as clean."
            );
        }
    }

    private Iterator<Volume> getVolumeIterator(VolumeDefinition vlmDfn)
    {
        Iterator<Volume> iterator;
        try
        {
            iterator = vlmDfn.iterateVolumes(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return iterator;
    }

    private boolean isMarkedAsClean(Volume vlm)
    {
        boolean isMarkedAsClean;
        try
        {
            isMarkedAsClean = vlm.getFlags().isSet(apiCtx, VlmFlags.CLEAN);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return isMarkedAsClean;
    }

    private boolean isMarkedForDeletion(VolumeDefinition vlmDfn)
    {
        boolean isMarkedAsDeleted;
        try
        {
            isMarkedAsDeleted = vlmDfn.getFlags().isSet(apiCtx, VlmDfnFlags.DELETE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return isMarkedAsDeleted;
    }

    private void delete(VolumeDefinition vlmDfn)
    {
        try
        {
            vlmDfn.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + CtrlVlmDfnApiCallHandler.getObjectDescriptionInline(
                    vlmDfn.getResourceDefinition().getName().displayValue,
                    vlmDfn.getVolumeNumber().value
                )
            );
        }
    }
}
