package com.linbit.linstor.core.apicallhandler.satellite;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.FreeSpaceMgrSatelliteFactory;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterfaceDataFactory;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeDataSatelliteFactory;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceConnectionDataSatelliteFactory;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDataSatelliteFactory;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionDataSatelliteFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDataSatelliteFactory;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionDataSatelliteFactory;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.VolumeDataFactory;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeDefinitionDataSatelliteFactory;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherNodeNetInterfacePojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherRscPojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.DivergentDataException;
import com.linbit.linstor.core.DivergentUuidsException;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.apicallhandler.LayerRscDataMerger;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

@Singleton
class StltRscApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final ResourceDefinitionDataSatelliteFactory resourceDefinitionDataFactory;
    private final VolumeDefinitionDataSatelliteFactory volumeDefinitionDataFactory;
    private final NodeDataSatelliteFactory nodeDataFactory;
    private final NetInterfaceDataFactory netInterfaceDataFactory;
    private final ResourceDataSatelliteFactory resourceDataFactory;
    private final StorPoolDefinitionDataSatelliteFactory storPoolDefinitionDataFactory;
    private final StorPoolDataSatelliteFactory storPoolDataFactory;
    private final VolumeDataFactory volumeDataFactory;
    private final ResourceConnectionDataSatelliteFactory resourceConnectionDataFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StltSecurityObjects stltSecObjs;
    private final FreeSpaceMgrSatelliteFactory freeSpaceMgrFactory;
    private final LayerRscDataMerger layerRscDataMerger;
    private final StltCryptApiCallHelper cryptHelper;

    @Inject
    StltRscApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        StorPoolDefinitionMap storPoolDfnMapRef,
        ResourceDefinitionDataSatelliteFactory resourceDefinitionDataFactoryRef,
        VolumeDefinitionDataSatelliteFactory volumeDefinitionDataFactoryRef,
        NodeDataSatelliteFactory nodeDataFactoryRef,
        NetInterfaceDataFactory netInterfaceDataFactoryRef,
        ResourceDataSatelliteFactory resourceDataFactoryRef,
        StorPoolDefinitionDataSatelliteFactory storPoolDefinitionDataFactoryRef,
        StorPoolDataSatelliteFactory storPoolDataFactoryRef,
        VolumeDataFactory volumeDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects stltSecObjsRef,
        ResourceConnectionDataSatelliteFactory resourceConnectionDataFactoryRef,
        FreeSpaceMgrSatelliteFactory freeSpaceMgrFactoryRef,
        LayerRscDataMerger layerRscDataMergerRef,
        StltCryptApiCallHelper cryptHelperRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        resourceDefinitionDataFactory = resourceDefinitionDataFactoryRef;
        volumeDefinitionDataFactory = volumeDefinitionDataFactoryRef;
        nodeDataFactory = nodeDataFactoryRef;
        netInterfaceDataFactory = netInterfaceDataFactoryRef;
        resourceDataFactory = resourceDataFactoryRef;
        storPoolDefinitionDataFactory = storPoolDefinitionDataFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
        volumeDataFactory = volumeDataFactoryRef;
        transMgrProvider = transMgrProviderRef;
        stltSecObjs = stltSecObjsRef;
        resourceConnectionDataFactory = resourceConnectionDataFactoryRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
        layerRscDataMerger = layerRscDataMergerRef;
        cryptHelper = cryptHelperRef;
    }

    /**
     * We requested an update to a resource and the controller is telling us that the requested resource
     * does no longer exist.
     * Basically we now just mark the update as received and applied to prevent the
     * {@link DeviceManager} from waiting for the update.
     *
     * @param rscNameStr
     */
    public void applyDeletedRsc(String rscNameStr)
    {
        try
        {
            ResourceName rscName = new ResourceName(rscNameStr);

            ResourceDefinition removedRscDfn = rscDfnMap.remove(rscName); // just to be sure
            if (removedRscDfn != null)
            {
                removedRscDfn.delete(apiCtx);
                transMgrProvider.get().commit();
            }

            errorReporter.logInfo("Resource definition '" + rscNameStr +
                "' and the corresponding resource removed by Controller.");

            deviceManager.rscUpdateApplied(
                Collections.singleton(
                    new Resource.Key(
                        controllerPeerConnector.getLocalNodeName(),
                        rscName
                    )
                )
            );
        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
    }

    public void applyChanges(RscPojo rscRawData)
    {
        try
        {
            ResourceName rscName;

            ResourceDefinitionData rscDfnToRegister = null;

            rscName = new ResourceName(rscRawData.getName());
            RscDfnFlags[] rscDfnFlags = RscDfnFlags.restoreFlags(rscRawData.getRscDfnFlags());

            ResourceDefinitionData rscDfn = (ResourceDefinitionData) rscDfnMap.get(rscName);

            Resource localRsc = null;
            Set<Resource> otherRscs = new HashSet<>();
            if (rscDfn == null)
            {
                rscDfn = resourceDefinitionDataFactory.getInstanceSatellite(
                    apiCtx,
                    rscRawData.getRscDfnUuid(),
                    rscName,
                    rscDfnFlags
                );

                checkUuid(rscDfn, rscRawData);
                rscDfnToRegister = rscDfn;
            }
            Props rscDfnProps = rscDfn.getProps(apiCtx);
            rscDfnProps.map().putAll(rscRawData.getRscDfnProps());
            rscDfnProps.keySet().retainAll(rscRawData.getRscDfnProps().keySet());
            rscDfn.getFlags().resetFlagsTo(apiCtx, rscDfnFlags);

            // merge vlmDfns
            {
                Map<VolumeNumber, VolumeDefinition> vlmDfnsToDelete = new TreeMap<>();

                for (VolumeDefinition.VlmDfnApi vlmDfnRaw : rscRawData.getVlmDfns())
                {
                    VlmDfnFlags[] vlmDfnFlags = VlmDfnFlags.restoreFlags(vlmDfnRaw.getFlags());
                    VolumeNumber vlmNr = new VolumeNumber(vlmDfnRaw.getVolumeNr());

                    VolumeDefinitionData vlmDfn = volumeDefinitionDataFactory.getInstanceSatellite(
                        apiCtx,
                        vlmDfnRaw.getUuid(),
                        rscDfn,
                        vlmNr,
                        vlmDfnRaw.getSize(),
                        VlmDfnFlags.restoreFlags(vlmDfnRaw.getFlags())
                    );
                    checkUuid(vlmDfn, vlmDfnRaw, rscName.displayValue);
                    Props vlmDfnProps = vlmDfn.getProps(apiCtx);
                    vlmDfnProps.map().putAll(vlmDfnRaw.getProps());
                    vlmDfnProps.keySet().retainAll(vlmDfnRaw.getProps().keySet());

                    vlmDfn.setVolumeSize(apiCtx, vlmDfnRaw.getSize());

                    // corresponding volumes will be created later when iterating over (local|remote)vlmApis

                    if (Arrays.asList(vlmDfnFlags).contains(VlmDfnFlags.DELETE))
                    {
                        vlmDfnsToDelete.put(vlmNr, vlmDfn);
                    }
                }

                for (Entry<VolumeNumber, VolumeDefinition> entry : vlmDfnsToDelete.entrySet())
                {
                     VolumeDefinition vlmDfn = entry.getValue();
                     Iterator<Volume> iterateVolumes = vlmDfn.iterateVolumes(apiCtx);
                     while (iterateVolumes.hasNext())
                     {
                         Volume vlm = iterateVolumes.next();
                         vlm.markDeleted(apiCtx);
                     }
                    vlmDfn.markDeleted(apiCtx);
                }
            }

            final List<NodeData> nodesToRegister = new ArrayList<>();
            final Set<Resource.Key> createdRscSet = new TreeSet<>();
            final Set<Resource.Key> updatedRscSet = new TreeSet<>();
            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            if (!rscIterator.hasNext())
            {
                // our rscDfn is empty
                // that means, just create everything we need

                NodeData localNode = controllerPeerConnector.getLocalNode();

                localRsc = createRsc(
                    rscRawData.getLocalRscUuid(),
                    localNode,
                    rscDfn,
                    RscFlags.restoreFlags(rscRawData.getLocalRscFlags()),
                    rscRawData.getLocalRscProps(),
                    rscRawData.getLocalVlms(),
                    false
                );

                createdRscSet.add(new Resource.Key(localRsc));

                for (OtherRscPojo otherRscRaw : rscRawData.getOtherRscList())
                {
                    NodeData remoteNode = nodeDataFactory.getInstanceSatellite(
                        apiCtx,
                        otherRscRaw.getNodeUuid(),
                        new NodeName(otherRscRaw.getNodeName()),
                        NodeType.valueOf(otherRscRaw.getNodeType()),
                        NodeFlag.restoreFlags(otherRscRaw.getNodeFlags())
                    );
                    checkUuid(remoteNode, otherRscRaw);

                    // set node's netinterfaces
                    for (OtherNodeNetInterfacePojo otherNodeNetIf : otherRscRaw.getNetInterfacefPojos())
                    {
                        netInterfaceDataFactory.getInstanceSatellite(
                            apiCtx,
                            otherNodeNetIf.getUuid(),
                            remoteNode,
                            new NetInterfaceName(otherNodeNetIf.getName()),
                            new LsIpAddress(otherNodeNetIf.getAddress())
                        );
                    }
                    nodesToRegister.add(remoteNode);

                    ResourceData remoteRsc = createRsc(
                        otherRscRaw.getRscUuid(),
                        remoteNode,
                        rscDfn,
                        RscFlags.restoreFlags(otherRscRaw.getRscFlags()),
                        otherRscRaw.getRscProps(),
                        otherRscRaw.getVlms(),
                        true
                    );
                    otherRscs.add(remoteRsc);

                    createdRscSet.add(new Resource.Key(remoteRsc));
                }
            }
            else
            {
                // iterator contains at least one resource.
                List<Resource> removedList = new ArrayList<>();

                // first we split the existing resources into our local resource and a list of others
                while (rscIterator.hasNext())
                {
                    Resource rsc = rscIterator.next();
                    if (rsc.getUuid().equals(rscRawData.getLocalRscUuid()))
                    {
                        localRsc = rsc;
                    }
                    else
                    {
                        removedList.add(rsc);
                    }
                    // TODO: resources will still remain in the rscDfn. maybe this will get a problem
                }
                // now we should have found our localRsc, and added all all other resources in removedList
                // we will delete them from the list as we find them with matching uuid and node names

                if (localRsc == null)
                {
                    throw new DivergentUuidsException(
                        String.format(
                            "The local resource with the UUID '%s' was not found in the stored " +
                                "resource definition '%s'.",
                            rscRawData.getLocalRscUuid().toString(),
                            rscName
                        )
                    );
                }

                // update volumes
                {

                    // we do not have to care about deletion, as the merge of vlmDfns should have already marked
                    // all the corresponding volumes for deletion
                    for (VlmApi vlmApi : rscRawData.getLocalVlms())
                    {
                        Volume vlm = localRsc.getVolume(new VolumeNumber(vlmApi.getVlmNr()));

                        if (vlm != null)
                        {
                            mergeVlm(vlm, vlmApi, false);
                        }
                        else
                        {
                            createVlm(vlmApi, localRsc, false);
                        }
                    }
                }

                // update props
                {
                    Props localRscProps = localRsc.getProps(apiCtx);
                    localRscProps.map().putAll(rscRawData.getLocalRscProps());
                    localRscProps.keySet().retainAll(rscRawData.getLocalRscProps().keySet());
                }

                // update flags
                localRsc.getStateFlags().resetFlagsTo(apiCtx, RscFlags.restoreFlags(rscRawData.getLocalRscFlags()));

                updatedRscSet.add(new Resource.Key(localRsc));

                for (OtherRscPojo otherRsc : rscRawData.getOtherRscList())
                {
                    Resource remoteRsc = null;

                    for (Resource removed : removedList)
                    {
                        if (otherRsc.getRscUuid().equals(removed.getUuid()))
                        {
                            if (!otherRsc.getNodeName().equals(
                                removed.getAssignedNode().getName().displayValue)
                            )
                            {
                                throw new DivergentDataException(
                                    "The resource with UUID '%s' was deployed on node '%s' but is now " +
                                        "on node '%s' (this should have cause a delete and re-deploy of that resource)."
                                );
                            }
                            if (!otherRsc.getNodeUuid().equals(
                                removed.getAssignedNode().getUuid())
                            )
                            {
                                throw new DivergentUuidsException(
                                    "Node",
                                    removed.getAssignedNode().getName().displayValue,
                                    otherRsc.getNodeName(),
                                    removed.getAssignedNode().getUuid(),
                                    otherRsc.getNodeUuid()
                                );
                            }

                            remoteRsc = removed;
                            break;
                        }
                    }
                    if (remoteRsc == null)
                    {
                        // controller sent us a resource that we don't know
                        // create its node
                        NodeName nodeName = new NodeName(otherRsc.getNodeName());
                        NodeData remoteNode = (NodeData) nodesMap.get(nodeName);
                        if (remoteNode == null)
                        {
                            remoteNode = nodeDataFactory.getInstanceSatellite(
                                apiCtx,
                                otherRsc.getNodeUuid(),
                                nodeName,
                                NodeType.valueOf(otherRsc.getNodeType()),
                                NodeFlag.restoreFlags(otherRsc.getNodeFlags())
                            );

                            // set node's netinterfaces
                            for (OtherNodeNetInterfacePojo otherNodeNetIf : otherRsc.getNetInterfacefPojos())
                            {
                                netInterfaceDataFactory.getInstanceSatellite(
                                    apiCtx,
                                    otherNodeNetIf.getUuid(),
                                    remoteNode,
                                    new NetInterfaceName(otherNodeNetIf.getName()),
                                    new LsIpAddress(otherNodeNetIf.getAddress())
                                );
                            }

                            nodesToRegister.add(remoteNode);
                        }
                        else
                        {
                            checkUuid(remoteNode, otherRsc);
                        }
                        Props remoteNodeProps = remoteNode.getProps(apiCtx);
                        remoteNodeProps.map().putAll(otherRsc.getNodeProps());
                        remoteNodeProps.keySet().retainAll(otherRsc.getNodeProps().keySet());

                        // create resource
                        remoteRsc = createRsc(
                            otherRsc.getRscUuid(),
                            remoteNode,
                            rscDfn,
                            RscFlags.restoreFlags(otherRsc.getRscFlags()),
                            otherRsc.getRscProps(),
                            otherRsc.getVlms(),
                            true
                        );

                        createdRscSet.add(new Resource.Key(remoteRsc));
                    }
                    else
                    {
                        // we found the resource by the uuid the controller sent us
                        Node remoteNode = remoteRsc.getAssignedNode();
                        // check if the node uuids also match
                        checkUuid(remoteNode, otherRsc);

                        // update node props
                        Props remoteNodeProps = remoteNode.getProps(apiCtx);
                        remoteNodeProps.map().putAll(otherRsc.getNodeProps());
                        remoteNodeProps.keySet().retainAll(otherRsc.getNodeProps().keySet());

                        // update matching resource props
                        Props remoteRscProps = remoteRsc.getProps(apiCtx);
                        remoteRscProps.map().putAll(otherRsc.getRscProps());
                        remoteRscProps.keySet().retainAll(otherRsc.getRscProps().keySet());

                        // update flags
                        remoteRsc.getStateFlags().resetFlagsTo(apiCtx, RscFlags.restoreFlags(otherRsc.getRscFlags()));

                        // update volumes
                        {
                            // we do not have to care about deletion, as the merge of vlmDfns should have already marked
                            // all the corresponding volumes for deletion
                            for (VlmApi remoteVlmApi : otherRsc.getVlms())
                            {
                                Volume vlm = remoteRsc.getVolume(new VolumeNumber(remoteVlmApi.getVlmNr()));
                                if (vlm == null)
                                {
                                    createVlm(remoteVlmApi, remoteRsc, true);
                                }
                                else
                                {
                                    mergeVlm(vlm, remoteVlmApi, true);
                                }
                            }
                        }

                        // everything ok, mark the resource to be kept
                        removedList.remove(remoteRsc);

                        updatedRscSet.add(new Resource.Key(remoteRsc));
                    }
                    if (remoteRsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DELETE))
                    {
                        // remote resources do not go through the device manager, which means
                        // we have to delete them here.
                        // otherwise the resource never gets deleted, which will cause a
                        // divergent UUID exception when the "same" remote resource gets
                        // recreated
                        Node remoteNode = remoteRsc.getAssignedNode();
                        remoteRsc.delete(apiCtx);

                        /*
                         *  Bugfix: if the remoteRsc was the last resource of the remote node
                         *  we will no longer receive updates about the remote node (why should we?)
                         *  The problem is, that if the remote node gets completely deleted
                         *  on the controller, and later recreated, and that "new" node deploys
                         *  a resource we are also interested in, we will receive the "new" node's UUID.
                         *  However, we will still find our old node-reference when looking up the
                         *  "new" node's name and therefor we will find the old node's UUID and check it
                         *  against the "new" node's UUID.
                         *  This will cause a UUID mismatch upon resource-creation on the other node
                         *  (which will trigger an update to us as we also need to know about the new resource
                         *  and it's node)
                         *
                         *  Therefore, we have to remove the remoteNode completely if it has no
                         *  resources left
                         */

                        if (!remoteNode.iterateResources(apiCtx).hasNext())
                        {
                            nodesMap.remove(remoteNode.getName());
                            remoteNode.delete(apiCtx);
                        }
                    }
                    else
                    {
                        otherRscs.add(remoteRsc);
                    }
                }
                // all resources have been created, updated or deleted

                if (!removedList.isEmpty())
                {
                    errorReporter.logWarning(
                        "We know at least one resource the controller does not:\n   " +
                            removedList.stream()
                                .map(Resource::toString)
                                .collect(Collectors.joining(",\n   ")) +
                            "\nThe controller is not be aware of typed resources or we have missed a resource deletion."
                    );
                }
            }

            // create resource connections
            for (ResourceConnection.RscConnApi rscConnApi : rscRawData.getRscConnections())
            {
                Resource sourceResource = rscDfn.getResource(apiCtx, new NodeName(rscConnApi.getSourceNodeName()));
                Resource targetResource = rscDfn.getResource(apiCtx, new NodeName(rscConnApi.getTargetNodeName()));

                /*
                 *  When the remote resource was just deleted within this call, the controller
                 *  still serialized the resource-connection between that remote resource and our local resource
                 *  as the controller only "marked" the remote resource for deletion.
                 *
                 *  As we just deleted that remote resource, the next lookup for that ResourceConnection could result
                 *  in a NPE if one of the resources is already deleted (unlinked from every Map).
                 */
                if (sourceResource != null && targetResource != null)
                {
                    ResourceConnection rscConn = resourceConnectionDataFactory.getInstanceSatellite(
                        apiCtx,
                        rscConnApi.getUuid(),
                        sourceResource,
                        targetResource,
                        new ResourceConnection.RscConnFlags[] {},
                        null
                    );

                    Props rscConnProps = rscConn.getProps(apiCtx);
                    rscConnProps.map().putAll(rscConnApi.getProps());
                    rscConnProps.keySet().retainAll(rscConnApi.getProps().keySet());

                    rscConn.getStateFlags().resetFlagsTo(
                        apiCtx, ResourceConnection.RscConnFlags.restoreFlags(rscConnApi.getFlags()));

                    rscConn.setPort(
                        apiCtx, rscConnApi.getPort() == null ? null : new TcpPortNumber(rscConnApi.getPort()));
                }
            }

            if (rscDfnToRegister != null)
            {
                rscDfnMap.put(rscName, rscDfnToRegister);
            }
            for (Node node : nodesToRegister)
            {
                if (!node.isDeleted())
                {
                    nodesMap.put(node.getName(), node);
                }
            }

            layerRscDataMerger.restoreLayerData(localRsc, rscRawData.getLayerData());

            cryptHelper.decryptAllNewLuksVlmKeys(false);

            for (Resource otherRsc : otherRscs)
            {
                OtherRscPojo otherRscPojo = null;
                for (OtherRscPojo tmp : rscRawData.getOtherRscList())
                {
                    if (otherRsc.getAssignedNode().getName().value.equals(
                        tmp.getNodeName().toUpperCase()
                    ))
                    {
                        otherRscPojo = tmp;
                        break;
                    }
                }
                if (otherRscPojo == null)
                {
                    throw new ImplementationError("No rscPojo found for remote resource " + otherRsc);
                }
                layerRscDataMerger.restoreLayerData(otherRsc, otherRscPojo.getRscLayerDataPojo());
            }

            transMgrProvider.get().commit();

            Set<Resource.Key> devMgrNotifications = new TreeSet<>();

            reportSuccess(createdRscSet, "created");
            reportSuccess(updatedRscSet, "updated");

            devMgrNotifications.addAll(createdRscSet);
            devMgrNotifications.addAll(updatedRscSet);

            deviceManager.rscUpdateApplied(devMgrNotifications);
        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
    }

    private void reportSuccess(Set<Resource.Key> rscSet, String action)
    {
        for (Resource.Key rscKey : rscSet)
        {
            StringBuilder msgBuilder = new StringBuilder();
            msgBuilder
                .append("Resource '")
                .append(rscKey.getResourceName().displayValue)
                .append("' ")
                .append(action)
                .append(" for node '")
                .append(rscKey.getNodeName().displayValue)
                .append("'.");

            errorReporter.logInfo(msgBuilder.toString());
        }
    }

    private ResourceData createRsc(
        UUID rscUuid,
        NodeData node,
        ResourceDefinitionData rscDfn,
        RscFlags[] flags,
        Map<String, String> rscProps,
        List<VolumeData.VlmApi> vlms,
        boolean remoteRsc
    )
        throws AccessDeniedException, ValueOutOfRangeException, InvalidNameException, DivergentDataException
    {
        ResourceData rsc = resourceDataFactory.getInstanceSatellite(
            apiCtx,
            rscUuid,
            node,
            rscDfn,
            flags
        );

        checkUuid(
            rsc.getUuid(),
            rscUuid,
            "Resource",
            rsc.toString(),
            "Node: '" + node.getName().displayValue + "', RscName: '" + rscDfn.getName().displayValue + "'"
        );

        Props rscDataProps = rsc.getProps(apiCtx);
        rscDataProps.map().putAll(rscProps);
        rscDataProps.keySet().retainAll(rscProps.keySet());

        // XXX ensure to apply possible changes in layerData

        for (Volume.VlmApi vlmRaw : vlms)
        {
            createVlm(vlmRaw, rsc, remoteRsc);
        }

        return rsc;
    }

    private void createVlm(
        VlmApi vlmApi,
        Resource rsc,
        boolean remoteRsc
    )
        throws AccessDeniedException, InvalidNameException, DivergentDataException, ValueOutOfRangeException
    {
        StorPool storPool = getStorPool(rsc, vlmApi, remoteRsc);

        VolumeDefinition vlmDfn = rsc.getDefinition().getVolumeDfn(apiCtx, new VolumeNumber(vlmApi.getVlmNr()));

        VolumeData vlm = volumeDataFactory.getInstanceSatellite(
            apiCtx,
            vlmApi.getVlmUuid(),
            rsc,
            vlmDfn,
            storPool,
            Volume.VlmFlags.restoreFlags(vlmApi.getFlags())
        );

        vlm.getProps(apiCtx).map().putAll(vlmApi.getVlmProps());

        // XXX check if vlm really has expected layerStack (stack cannot be changed!)
    }

    private void mergeVlm(Volume vlm, VlmApi vlmApi, boolean remoteRsc)
        throws DivergentDataException, AccessDeniedException, SQLException, InvalidNameException
    {
        if (!remoteRsc)
        {
            checkUuid(vlm, vlmApi);
        }
        StorPool storPool = getStorPool(vlm.getResource(), vlmApi, remoteRsc);
        vlm.setStorPool(apiCtx, storPool);

        Props vlmProps = vlm.getProps(apiCtx);
        vlmProps.map().putAll(vlmApi.getVlmProps());
        vlmProps.keySet().retainAll(vlmApi.getVlmProps().keySet());
        vlm.getFlags().resetFlagsTo(apiCtx, Volume.VlmFlags.restoreFlags(vlmApi.getFlags()));
    }

    private StorPool getStorPool(Resource rsc, VlmApi vlmApi, boolean remoteRsc)
        throws AccessDeniedException, InvalidNameException, DivergentDataException
    {
        StorPool storPool = rsc.getAssignedNode().getStorPool(
            apiCtx,
            new StorPoolName(vlmApi.getStorPoolName())
        );

        if (storPool == null)
        {
            if (remoteRsc)
            {
                StorPoolDefinition storPoolDfn =
                    storPoolDfnMap.get(new StorPoolName(vlmApi.getStorPoolName()));
                if (storPoolDfn == null)
                {
                    storPoolDfn = storPoolDefinitionDataFactory.getInstance(
                        apiCtx,
                        vlmApi.getStorPoolDfnUuid(),
                        new StorPoolName(vlmApi.getStorPoolName())
                    );

                    storPoolDfn.getProps(apiCtx).map().putAll(vlmApi.getStorPoolDfnProps());

                    storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);
                }

                storPool = storPoolDataFactory.getInstanceSatellite(
                    apiCtx,
                    vlmApi.getStorPoolUuid(),
                    rsc.getAssignedNode(),
                    storPoolDfn,
                    vlmApi.getStorPoolDeviceProviderKind(),
                    freeSpaceMgrFactory.getInstance()
                );
                storPool.getProps(apiCtx).map().putAll(vlmApi.getStorPoolProps());
            }
            else
            {
                throw new DivergentDataException("Unknown StorPool: '" + vlmApi.getStorPoolName() + "'");
            }
        }
        if (!remoteRsc && !storPool.getUuid().equals(vlmApi.getStorPoolUuid()))
        {
            throw new DivergentUuidsException(
                "StorPool",
                storPool.toString(),
                vlmApi.getStorPoolName(),
                storPool.getUuid(),
                vlmApi.getStorPoolUuid()
            );
        }

        Props storPoolProps = storPool.getProps(apiCtx);
        storPoolProps.map().putAll(vlmApi.getStorPoolProps());
        storPoolProps.keySet().retainAll(vlmApi.getStorPoolProps().keySet());

        return storPool;
    }

    private void checkUuid(Node node, OtherRscPojo otherRsc)
        throws DivergentUuidsException
    {
        checkUuid(
            node.getUuid(),
            otherRsc.getNodeUuid(),
            "Node",
            node.getName().displayValue,
            otherRsc.getNodeName()
        );
    }

    private void checkUuid(ResourceDefinition rscDfn, RscPojo rscRawData)
        throws DivergentUuidsException
    {
        checkUuid(
            rscDfn.getUuid(),
            rscRawData.getRscDfnUuid(),
            "ResourceDefinition",
            rscDfn.getName().displayValue,
            rscRawData.getName()
        );
    }

    private void checkUuid(Resource rsc, OtherRscPojo otherRsc, String otherRscName)
        throws DivergentUuidsException
    {
        checkUuid(
            rsc.getUuid(),
            otherRsc.getRscUuid(),
            "Resource",
            String.format("Node: '%s', Rsc: '%s'",
                rsc.getAssignedNode().getName().displayValue,
                rsc.getDefinition().getName().displayValue
            ),
            String.format("Node: '%s', Rsc: '%s'",
                otherRsc.getNodeName(),
                otherRscName
            )
        );
    }

    private void checkUuid(
        VolumeDefinition vlmDfn,
        VolumeDefinition.VlmDfnApi vlmDfnRaw,
        String remoteRscName
    )
        throws DivergentUuidsException
    {
        checkUuid(
            vlmDfn.getUuid(),
            vlmDfnRaw.getUuid(),
            "VolumeDefinition",
            vlmDfn.toString(),
            String.format(
                "Rsc: '%s', VlmNr: '%d'",
                remoteRscName,
                vlmDfnRaw.getVolumeNr()
            )
        );
    }

    private void checkUuid(
        Volume vlm,
        VlmApi vlmRaw
    )
        throws DivergentUuidsException
    {
        checkUuid(
            vlm.getUuid(),
            vlmRaw.getVlmUuid(),
            "Volume",
            vlm.getKey().toString(),
            String.format(
                "Rsc: '%s', VlmNr: '%d'",
                vlm.getResource().getDefinition().getName().displayValue,
                vlmRaw.getVlmNr()
            )
        );
    }

    private void checkUuid(UUID localUuid, UUID remoteUuid, String type, String localName, String remoteName)
        throws DivergentUuidsException
    {
        if (!localUuid.equals(remoteUuid))
        {
            throw new DivergentUuidsException(
                type,
                localName,
                remoteName,
                localUuid,
                remoteUuid
            );
        }
    }
}
