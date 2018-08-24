package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinitionRepository;
import com.linbit.linstor.StorPool;
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
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import static com.linbit.linstor.api.ApiConsts.API_LST_VLM;
import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlVlmApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final NodeRepository nodeRepository;
    private final CtrlClientSerializer clientComSerializer;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlVlmApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        NodeRepository nodeRepositoryRef,
        CtrlClientSerializer clientComSerializerRef,
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
        nodeRepository = nodeRepositoryRef;
        clientComSerializer = clientComSerializerRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    ApiCallRc volumeResized(
        String nodeNameStr,
        String rscNameStr,
        int volumeNr,
        long vlmSize
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeVlmContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr,
            volumeNr
        );

        try
        {
            ResourceData rscData = ctrlApiDataLoader.loadRsc(nodeNameStr, rscNameStr, true);
            VolumeNumber volumeNumber = LinstorParsingUtils.asVlmNr(volumeNr);

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
                    // This can occur when multiple resize commands are being executed concurrently.
                    // E.g. when the controller receives a command to resize a volume to size X and then one to resize
                    // to size Y, the first resize notification will have size X while the expected size will be Y.
                    errorReporter.logWarning(
                        "Volume %s resized to %d, awaiting resize to %d.", vlm, vlmSize, expectedSize);
                }
            }

            ctrlTransactionHelper.commit();

            if (updateSatellites)
            {
                responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(rscData));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    ApiCallRc volumeDrbdResized(
        String nodeNameStr,
        String rscNameStr,
        int volumeNr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeVlmContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr,
            volumeNr
        );

        try
        {
            ResourceData rscData = ctrlApiDataLoader.loadRsc(nodeNameStr, rscNameStr, true);
            VolumeNumber volumeNumber = LinstorParsingUtils.asVlmNr(volumeNr);

            Volume vlm = rscData.getVolume(volumeNumber);

            vlm.getFlags().disableFlags(apiCtx, VlmFlags.DRBD_RESIZE);
            vlm.getVolumeDefinition().getFlags().disableFlags(peerAccCtx.get(), VlmDfnFlags.RESIZE);

            ctrlTransactionHelper.commit();
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    ApiCallRc volumeDeleted(
        String nodeNameStr,
        String rscNameStr,
        int volumeNr,
        long freespace
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeVlmContext(
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            rscNameStr,
            volumeNr
        );

        try
        {
            ResourceData rscData = ctrlApiDataLoader.loadRsc(nodeNameStr, rscNameStr, true);
            VolumeNumber volumeNumber = LinstorParsingUtils.asVlmNr(volumeNr);

            Volume vlm = rscData.getVolume(volumeNumber);
            UUID vlmUuid = vlm.getUuid(); // prevent access to deleted object

            markClean(vlm);

            deleteFromStorPool(vlm, freespace);

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

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultDeletedEntry(
                vlmUuid, getVlmDescriptionInline(rscData, vlmDfn)));
            errorReporter.logDebug(
                String.format("Volume with number '%d' deleted on node '%s'.", volumeNr, nodeNameStr)
            );
            if (vlmDfnDeleted)
            {
                responseConverter.addWithOp(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.DELETED,
                        "Volume definition with number '" + volumeNumber.value + "' of resource definition '" +
                            rscData.getDefinition().getName().displayValue + "' deleted."
                    )
                    .setDetails(
                        "Volume definition's UUID was: " + vlmDfnUuid.toString()
                    )
                    .build());
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private void deleteFromStorPool(Volume vlm, long freeSpace)
    {
        StorPool storPool = null;
        try
        {
            storPool = vlm.getStorPool(peerAccCtx.get());
            storPool.removeVolume(peerAccCtx.get(), vlm);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "deleting " + getVlmDescriptionInline(vlm),
                storPool == null ? ApiConsts.FAIL_ACC_DENIED_STOR_POOL :
                    ApiConsts.FAIL_ACC_DENIED_FREE_SPACE_MGR
            );
        }
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

    private void markClean(Volume vol)
    {
        try
        {
            vol.getFlags().enableFlags(apiCtx, VlmFlags.CLEAN);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
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
            throw new ImplementationError(accDeniedExc);
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
            throw new ImplementationError(accDeniedExc);
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
            throw new ImplementationError(accDeniedExc);
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
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    public static String getVlmDescription(Volume vlm)
    {
        return getVlmDescription(
            vlm.getResource().getAssignedNode().getName().displayValue,
            vlm.getResourceDefinition().getName().displayValue,
            vlm.getVolumeDefinition().getVolumeNumber().value
        );
    }

    public static String getVlmDescription(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "Node: " + nodeNameStr + ", Resource: " + rscNameStr +
            " Volume number: " + vlmNr;
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

    private static ResponseContext makeVlmContext(
        ApiOperation operation,
        String nodeNameStr,
        String rscNameStr,
        int volumeNr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        objRefs.put(ApiConsts.KEY_VLM_NR, Integer.toString(volumeNr));

        return new ResponseContext(
            operation,
            getVlmDescription(nodeNameStr, rscNameStr, volumeNr),
            getVlmDescriptionInline(nodeNameStr, rscNameStr, volumeNr),
            ApiConsts.MASK_VLM,
            objRefs
        );
    }
}
