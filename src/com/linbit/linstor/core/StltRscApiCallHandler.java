package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.NetInterfaceDataFactory;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeDataSatelliteFactory;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDataFactory;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionDataSatelliteFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDataFactory;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionDataFactory;
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
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
    private final ResourceDataFactory resourceDataFactory;
    private final StorPoolDefinitionDataFactory storPoolDefinitionDataFactory;
    private final StorPoolDataFactory storPoolDataFactory;
    private final VolumeDataFactory volumeDataFactory;
    private final Provider<TransactionMgr> transMgrProvider;

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
        ResourceDataFactory resourceDataFactoryRef,
        StorPoolDefinitionDataFactory storPoolDefinitionDataFactoryRef,
        StorPoolDataFactory storPoolDataFactoryRef,
        VolumeDataFactory volumeDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
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
                "' and the corresponding resource" + " removed by Controller.");

            Map<ResourceName, Set<NodeName>> updatedRscs = new TreeMap<>();
            updatedRscs.put(rscName, new TreeSet<NodeName>());
            deviceManager.rscUpdateApplied(updatedRscs);

            Set<ResourceName> rscDfnSet = new TreeSet<>();
            rscDfnSet.add(rscName);
            deviceManager.rscDefUpdateApplied(rscDfnSet);
        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
    }

    @SuppressWarnings("checkstyle:variabledeclarationusagedistance")
    public void applyChanges(RscPojo rscRawData)
    {
        try
        {
            ResourceName rscName;

            ResourceDefinitionData rscDfnToRegister = null;
            List<NodeData> nodesToRegister = new ArrayList<>();

            Map<ResourceName, Set<NodeName>> createdRscMap = new TreeMap<>();
            Map<ResourceName, Set<NodeName>> updatedRscMap = new TreeMap<>();

            rscName = new ResourceName(rscRawData.getName());
            String rscDfnSecret = rscRawData.getRscDfnSecret();
            TransportType rscDfnTransportType = TransportType.byValue(rscRawData.getRscDfnTransportType());
            TcpPortNumber port = new TcpPortNumber(rscRawData.getRscDfnPort());
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
                    port,
                    rscDfnFlags,
                    rscDfnSecret,
                    rscDfnTransportType
                );

                checkUuid(rscDfn, rscRawData);
                rscDfnToRegister = rscDfn;
            }
            rscDfn.setPort(apiCtx, port);
            Map<String, String> rscDfnProps = rscDfn.getProps(apiCtx).map();
            rscDfnProps.clear();
            rscDfnProps.putAll(rscRawData.getRscDfnProps());
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
                        new MinorNumber(vlmDfnRaw.getMinorNr()),
                        VlmDfnFlags.restoreFlags(vlmDfnRaw.getFlags())
                    );
                    checkUuid(vlmDfn, vlmDfnRaw, rscName.displayValue);
                    Map<String, String> vlmDfnPropsMap = vlmDfn.getProps(apiCtx).map();
                    vlmDfnPropsMap.clear();
                    vlmDfnPropsMap.putAll(vlmDfnRaw.getProps());

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
                    new NodeId(rscRawData.getLocalRscNodeId()),
                    RscFlags.restoreFlags(rscRawData.getLocalRscFlags()),
                    rscRawData.getLocalRscProps(),
                    rscRawData.getLocalVlms(),
                    false
                );

                add(localRsc, createdRscMap);

                for (OtherRscPojo otherRscRaw : rscRawData.getOtherRscList())
                {
                    NodeData remoteNode = nodeDataFactory.getInstanceSatellite(
                        apiCtx,
                        otherRscRaw.getNodeUuid(),
                        new NodeName(otherRscRaw.getNodeName()),
                        NodeType.valueOf(otherRscRaw.getNodeType()),
                        NodeFlag.restoreFlags(otherRscRaw.getNodeFlags()),
                        otherRscRaw.getNodeDisklessStorPoolUuid(),
                        controllerPeerConnector.getDisklessStorPoolDfn()
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
                        new NodeId(otherRscRaw.getRscNodeId()),
                        RscFlags.restoreFlags(otherRscRaw.getRscFlags()),
                        otherRscRaw.getRscProps(),
                        otherRscRaw.getVlms(),
                        true
                    );
                    otherRscs.add(remoteRsc);

                    add(remoteRsc, createdRscMap);
                }
            }
            else
            {
                // iterator contains at least one resource.
                List<Resource> removedList = new ArrayList<>();
                List<Resource> newResources = new ArrayList<>();
                List<Resource> modifiedResources = new ArrayList<>();
                List<Node> nodesToRemove = new ArrayList<>();

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
                        if (vlm == null)
                        {
                            createVlm(vlmApi, localRsc, false);
                        }
                    }
                }

                // update props
                {
                    Map<String, String> localRscProps = localRsc.getProps(apiCtx).map();
                    localRscProps.clear();
                    localRscProps.putAll(rscRawData.getLocalRscProps());
                }

                // update flags
                localRsc.getStateFlags().resetFlagsTo(apiCtx, RscFlags.restoreFlags(rscRawData.getLocalRscFlags()));

                add(localRsc, updatedRscMap);

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
                                NodeFlag.restoreFlags(otherRsc.getNodeFlags()),
                                otherRsc.getNodeDisklessStorPoolUuid(),
                                controllerPeerConnector.getDisklessStorPoolDfn()
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
                        Map<String, String> map = remoteNode.getProps(apiCtx).map();
                        map.clear();
                        map.putAll(otherRsc.getNodeProps());

                        // create resource
                        remoteRsc = createRsc(
                            otherRsc.getRscUuid(),
                            remoteNode,
                            rscDfn,
                            new NodeId(otherRsc.getRscNodeId()),
                            RscFlags.restoreFlags(otherRsc.getRscFlags()),
                            otherRsc.getRscProps(),
                            otherRsc.getVlms(),
                            true
                        );

                        // everything ok, mark the resource as new
                        newResources.add(remoteRsc);

                        add(remoteRsc, createdRscMap);
                    }
                    else
                    {
                        // we found the resource by the uuid the controller sent us
                        Node remoteNode = remoteRsc.getAssignedNode();
                        // check if the node uuids also match
                        checkUuid(remoteNode, otherRsc);

                        // update node props
                        Map<String, String> remoteNodeProps = remoteNode.getProps(apiCtx).map();
                        remoteNodeProps.clear();
                        remoteNodeProps.putAll(otherRsc.getNodeProps());

                        // update matching resource props
                        Map<String, String> remoteRscProps = remoteRsc.getProps(apiCtx).map();
                        remoteRscProps.clear();
                        remoteRscProps.putAll(otherRsc.getRscProps());

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
                                    StorPool remoteVlmStorPool = vlm.getStorPool(apiCtx);
                                    remoteVlmStorPool.getProps(apiCtx).map().putAll(remoteVlmApi.getStorPoolProps());
                                }
                            }
                        }

                        // everything ok, mark the resource to be kept
                        removedList.remove(remoteRsc);
                        modifiedResources.add(remoteRsc);

                        add(remoteRsc, updatedRscMap);
                    }
                    otherRscs.add(remoteRsc);
                }
                // all resources have been created or updated

                // cleanup

                // first, iterate over all resources marked for deletion and unlink them from rscDfn and node
                for (Resource rsc : removedList)
                {
                    rsc.markDeleted(apiCtx);
                }
            }

            if (rscDfnToRegister != null)
            {
                rscDfnMap.put(rscName, rscDfnToRegister);
            }
            for (Node node : nodesToRegister)
            {
                nodesMap.put(node.getName(), node);
            }

            transMgrProvider.get().commit();

            Map<ResourceName, Set<NodeName>> devMgrNotifications = new TreeMap<>();

            reportSuccess(createdRscMap, "created");
            reportSuccess(updatedRscMap, "updated");

            devMgrNotifications.putAll(createdRscMap);
            devMgrNotifications.putAll(updatedRscMap);

            deviceManager.rscUpdateApplied(devMgrNotifications);

        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
    }

    private void reportSuccess(Map<ResourceName, Set<NodeName>> map, String action)
    {
        for (Entry<ResourceName, Set<NodeName>> entry : map.entrySet())
        {
            Set<NodeName> nodeNames = entry.getValue();
            StringBuilder msgBuilder = new StringBuilder();
            msgBuilder
                .append("Resource '")
                .append(entry.getKey().displayValue)
                .append("' ")
                .append(action)
                .append(" for node");
            if (nodeNames.size() > 1)
            {
                msgBuilder.append("s");
            }
            msgBuilder.append(" '");
            for (NodeName nodeName : nodeNames)
            {
                msgBuilder
                    .append(nodeName.displayValue)
                    .append("', '");
            }
            msgBuilder.setLength(msgBuilder.length() - ", '".length());
            msgBuilder.append(".");

            errorReporter.logInfo(msgBuilder.toString());
        }
    }

    private void add(Resource rsc, Map<ResourceName, Set<NodeName>> map)
    {
        Set<NodeName> set = map.get(rsc.getDefinition().getName());
        if (set == null)
        {
            set = new TreeSet<>();
            map.put(rsc.getDefinition().getName(), set);
        }
        set.add(rsc.getAssignedNode().getName());
    }

    private ResourceData createRsc(
        UUID rscUuid,
        NodeData node,
        ResourceDefinitionData rscDfn,
        NodeId nodeId,
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
            nodeId,
            flags
        );

        checkUuid(
            rsc.getUuid(),
            rscUuid,
            "Resource",
            rsc.toString(),
            "Node: '" + node.getName().displayValue + "', RscName: '" + rscDfn.getName().displayValue + "'"
        );

        Map<String, String> map = rsc.getProps(apiCtx).map();
        map.clear();
        map.putAll(rscProps);

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
                    storPoolDfn = storPoolDefinitionDataFactory.getInstanceSatellite(
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
                    vlmApi.getStorDriverSimpleClassName()
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

        VolumeDefinition vlmDfn = rsc.getDefinition().getVolumeDfn(apiCtx, new VolumeNumber(vlmApi.getVlmNr()));

        volumeDataFactory.getInstanceSatellite(
            apiCtx,
            vlmApi.getVlmUuid(),
            rsc,
            vlmDfn,
            storPool,
            vlmApi.getBlockDevice(),
            vlmApi.getMetaDisk(),
            Volume.VlmFlags.restoreFlags(vlmApi.getFlags())
        );
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
        VolumeDefinitionData vlmDfn,
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
        VolumeData vlm,
        Volume.VlmApi vlmRaw,
        String remoteNodeName,
        String remoteRscName
    )
        throws DivergentUuidsException
    {
        checkUuid(
            vlm.getUuid(),
            vlmRaw.getVlmUuid(),
            "Volume",
            vlm.toString(),
            String.format(
                "Node: '%s', Rsc: '%s', VlmNr: '%d'",
                remoteNodeName,
                remoteRscName,
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

    private static class UpdatedObjects
    {
        private final Map<ResourceName, Set<NodeName>> createdRscMap;
        private final Map<ResourceName, Set<NodeName>> updatedRscMap;

        UpdatedObjects(
            Map<ResourceName, Set<NodeName>> createdRscMapRef,
            Map<ResourceName, Set<NodeName>> updatedRscMapRef
        )
        {
            createdRscMap = createdRscMapRef;
            updatedRscMap = updatedRscMapRef;
        }
    }
}
