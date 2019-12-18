package com.linbit.linstor.core.apicallhandler.satellite;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherNodeNetInterfacePojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherRscPojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.DivergentDataException;
import com.linbit.linstor.core.DivergentUuidsException;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.apicallhandler.StltLayerRscDataMerger;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgrSatelliteFactory;
import com.linbit.linstor.core.objects.NetInterfaceFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeSatelliteFactory;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceConnectionSatelliteFactory;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.ResourceSatelliteFactory;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.StorPoolSatelliteFactory;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.VolumeFactory;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

@Singleton
class StltRscApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceGroupMap rscGrpMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final ResourceDefinitionSatelliteFactory resourceDefinitionFactory;
    private final VolumeDefinitionSatelliteFactory volumeDefinitionFactory;
    private final NodeSatelliteFactory nodeFactory;
    private final NetInterfaceFactory netInterfaceFactory;
    private final ResourceSatelliteFactory resourceFactory;
    private final StorPoolDefinitionSatelliteFactory storPoolDefinitionFactory;
    private final StorPoolSatelliteFactory storPoolFactory;
    private final VolumeFactory volumeFactory;
    private final ResourceConnectionSatelliteFactory resourceConnectionFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StltSecurityObjects stltSecObjs;
    private final FreeSpaceMgrSatelliteFactory freeSpaceMgrFactory;
    private final StltRscGrpApiCallHelper rscGrpApiCallHelper;
    private final StltLayerRscDataMerger layerRscDataMerger;
    private final StltCryptApiCallHelper cryptHelper;

    @Inject
    StltRscApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceGroupMap rscGrpMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        StorPoolDefinitionMap storPoolDfnMapRef,
        ResourceDefinitionSatelliteFactory resourceDefinitionFactoryRef,
        VolumeDefinitionSatelliteFactory volumeDefinitionFactoryRef,
        NodeSatelliteFactory nodeFactoryRef,
        NetInterfaceFactory netInterfaceFactoryRef,
        ResourceSatelliteFactory resourceFactoryRef,
        StorPoolDefinitionSatelliteFactory storPoolDefinitionFactoryRef,
        StorPoolSatelliteFactory storPoolFactoryRef,
        VolumeFactory volumeFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects stltSecObjsRef,
        ResourceConnectionSatelliteFactory resourceConnectionFactoryRef,
        FreeSpaceMgrSatelliteFactory freeSpaceMgrFactoryRef,
        StltRscGrpApiCallHelper rscGrpApiCallHelperRef,
        StltLayerRscDataMerger layerRscDataMergerRef,
        StltCryptApiCallHelper cryptHelperRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        nodesMap = nodesMapRef;
        rscGrpMap = rscGrpMapRef;
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        resourceDefinitionFactory = resourceDefinitionFactoryRef;
        volumeDefinitionFactory = volumeDefinitionFactoryRef;
        nodeFactory = nodeFactoryRef;
        netInterfaceFactory = netInterfaceFactoryRef;
        resourceFactory = resourceFactoryRef;
        storPoolDefinitionFactory = storPoolDefinitionFactoryRef;
        storPoolFactory = storPoolFactoryRef;
        volumeFactory = volumeFactoryRef;
        transMgrProvider = transMgrProviderRef;
        stltSecObjs = stltSecObjsRef;
        resourceConnectionFactory = resourceConnectionFactoryRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
        rscGrpApiCallHelper = rscGrpApiCallHelperRef;
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
                ResourceGroup rscGrp = removedRscDfn.getResourceGroup();
                removedRscDfn.delete(apiCtx);
                if (!rscGrp.hasResourceDefinitions(apiCtx))
                {
                    rscGrpMap.remove(rscGrp.getName());
                    rscGrp.delete(apiCtx);
                }
                transMgrProvider.get().commit();
            }

            errorReporter.logInfo("Resource definition '" + rscNameStr +
                "' and the corresponding resource removed by Controller.");

            deviceManager.rscUpdateApplied(
                Collections.singleton(
                    new Resource.ResourceKey(
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

            ResourceDefinition rscDfnToRegister = null;

            rscName = new ResourceName(rscRawData.getName());
            ResourceDefinition.Flags[] rscDfnFlags = ResourceDefinition.Flags.restoreFlags(rscRawData.getRscDfnFlags());

            ResourceDefinition rscDfn = rscDfnMap.get(rscName);

            ResourceGroup rscGrp = rscGrpApiCallHelper.mergeResourceGroup(
                rscRawData.getRscDfnApi().getResourceGroup()
            );

            Resource localRsc = null;
            if (rscDfn == null)
            {
                rscDfn = resourceDefinitionFactory.getInstanceSatellite(
                    apiCtx,
                    rscRawData.getRscDfnUuid(),
                    rscGrp,
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

                for (VolumeDefinitionApi vlmDfnRaw : rscRawData.getVlmDfns())
                {
                    VolumeDefinition.Flags[] vlmDfnFlags = VolumeDefinition.Flags.restoreFlags(vlmDfnRaw.getFlags());
                    VolumeNumber vlmNr = new VolumeNumber(vlmDfnRaw.getVolumeNr());

                    VolumeDefinition vlmDfn = volumeDefinitionFactory.getInstanceSatellite(
                        apiCtx,
                        vlmDfnRaw.getUuid(),
                        rscDfn,
                        vlmNr,
                        vlmDfnRaw.getSize(),
                        VolumeDefinition.Flags.restoreFlags(vlmDfnRaw.getFlags())
                    );
                    checkUuid(vlmDfn, vlmDfnRaw, rscName.displayValue);
                    Props vlmDfnProps = vlmDfn.getProps(apiCtx);
                    vlmDfnProps.map().putAll(vlmDfnRaw.getProps());
                    vlmDfnProps.keySet().retainAll(vlmDfnRaw.getProps().keySet());

                    vlmDfn.setVolumeSize(apiCtx, vlmDfnRaw.getSize());

                    // corresponding volumes will be created later when iterating over (local|remote)vlmApis

                    if (Arrays.asList(vlmDfnFlags).contains(VolumeDefinition.Flags.DELETE))
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

            final List<Node> nodesToRegister = new ArrayList<>();
            final Set<Resource.ResourceKey> createdRscSet = new TreeSet<>();
            final Set<Resource.ResourceKey> updatedRscSet = new TreeSet<>();
            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            if (!rscIterator.hasNext())
            {
                // our rscDfn is empty
                // that means, just create everything we need

                Node localNode = controllerPeerConnector.getLocalNode();

                localRsc = createRsc(
                    rscRawData.getLocalRscUuid(),
                    localNode,
                    rscDfn,
                    Resource.Flags.restoreFlags(rscRawData.getLocalRscFlags()),
                    rscRawData.getLocalRscProps(),
                    rscRawData.getLocalVlms(),
                    false,
                    rscRawData.getLayerData()
                );

                createdRscSet.add(new Resource.ResourceKey(localRsc));

                for (OtherRscPojo otherRscRaw : rscRawData.getOtherRscList())
                {
                    Node remoteNode = nodeFactory.getInstanceSatellite(
                        apiCtx,
                        otherRscRaw.getNodeUuid(),
                        new NodeName(otherRscRaw.getNodeName()),
                        Node.Type.valueOf(otherRscRaw.getNodeType()),
                        Node.Flags.restoreFlags(otherRscRaw.getNodeFlags())
                    );
                    checkUuid(remoteNode, otherRscRaw);
                    remoteNode.getProps(apiCtx).map().putAll(otherRscRaw.getNodeProps());

                    // set node's netinterfaces
                    for (OtherNodeNetInterfacePojo otherNodeNetIf : otherRscRaw.getNetInterfacefPojos())
                    {
                        netInterfaceFactory.getInstanceSatellite(
                            apiCtx,
                            otherNodeNetIf.getUuid(),
                            remoteNode,
                            new NetInterfaceName(otherNodeNetIf.getName()),
                            new LsIpAddress(otherNodeNetIf.getAddress())
                        );
                    }
                    nodesToRegister.add(remoteNode);

                    Resource remoteRsc = createRsc(
                        otherRscRaw.getRscUuid(),
                        remoteNode,
                        rscDfn,
                        Resource.Flags.restoreFlags(otherRscRaw.getRscFlags()),
                        otherRscRaw.getRscProps(),
                        otherRscRaw.getVlms(),
                        true,
                        otherRscRaw.getRscLayerDataPojo()
                    );

                    layerRscDataMerger.mergeLayerData(remoteRsc, otherRscRaw.getRscLayerDataPojo(), true);

                    createdRscSet.add(new Resource.ResourceKey(remoteRsc));
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
                    for (VolumeApi vlmApi : rscRawData.getLocalVlms())
                    {
                        Volume localVlm = localRsc.getVolume(new VolumeNumber(vlmApi.getVlmNr()));

                        if (localVlm != null)
                        {
                            mergeVlm(localVlm, vlmApi, false);
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
                localRsc.getStateFlags().resetFlagsTo(
                    apiCtx,
                    Resource.Flags.restoreFlags(rscRawData.getLocalRscFlags())
                );

                updatedRscSet.add(new Resource.ResourceKey(localRsc));

                for (OtherRscPojo otherRsc : rscRawData.getOtherRscList())
                {
                    Resource remoteRsc = null;

                    for (Resource removed : removedList)
                    {
                        if (otherRsc.getRscUuid().equals(removed.getUuid()))
                        {
                            if (!otherRsc.getNodeName().equals(
                                removed.getNode().getName().displayValue)
                            )
                            {
                                throw new DivergentDataException(
                                    "The resource with UUID '%s' was deployed on node '%s' but is now " +
                                        "on node '%s' (this should have cause a delete and re-deploy of that resource)."
                                );
                            }
                            if (!otherRsc.getNodeUuid().equals(
                                removed.getNode().getUuid())
                            )
                            {
                                throw new DivergentUuidsException(
                                    "Node",
                                    removed.getNode().getName().displayValue,
                                    otherRsc.getNodeName(),
                                    removed.getNode().getUuid(),
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
                        Node remoteNode = nodesMap.get(nodeName);
                        if (remoteNode == null)
                        {
                            remoteNode = nodeFactory.getInstanceSatellite(
                                apiCtx,
                                otherRsc.getNodeUuid(),
                                nodeName,
                                Node.Type.valueOf(otherRsc.getNodeType()),
                                Node.Flags.restoreFlags(otherRsc.getNodeFlags())
                            );

                            // set node's netinterfaces
                            for (OtherNodeNetInterfacePojo otherNodeNetIf : otherRsc.getNetInterfacefPojos())
                            {
                                netInterfaceFactory.getInstanceSatellite(
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
                            Resource.Flags.restoreFlags(otherRsc.getRscFlags()),
                            otherRsc.getRscProps(),
                            otherRsc.getVlms(),
                            true,
                            otherRsc.getRscLayerDataPojo()
                        );

                        createdRscSet.add(new Resource.ResourceKey(remoteRsc));
                    }
                    else
                    {
                        // we found the resource by the uuid the controller sent us
                        Node remoteNode = remoteRsc.getNode();
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
                        remoteRsc.getStateFlags().resetFlagsTo(
                            apiCtx,
                            Resource.Flags.restoreFlags(otherRsc.getRscFlags())
                        );

                        // update volumes
                        {
                            // we do not have to care about deletion, as the merge of vlmDfns should have already marked
                            // all the corresponding volumes for deletion
                            for (VolumeApi remoteVlmApi : otherRsc.getVlms())
                            {
                                Volume remoteVlm = remoteRsc.getVolume(new VolumeNumber(remoteVlmApi.getVlmNr()));
                                if (remoteVlm == null)
                                {
                                    createVlm(remoteVlmApi, remoteRsc, true);
                                }
                                else
                                {
                                    mergeVlm(remoteVlm, remoteVlmApi, true);
                                }
                            }
                        }

                        // everything ok, mark the resource to be kept
                        removedList.remove(remoteRsc);

                        updatedRscSet.add(new Resource.ResourceKey(remoteRsc));
                    }
                    layerRscDataMerger.mergeLayerData(remoteRsc, otherRsc.getRscLayerDataPojo(), true);
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
            for (ResourceConnectionApi rscConnApi : rscRawData.getRscConnections())
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
                    ResourceConnection rscConn = resourceConnectionFactory.getInstanceSatellite(
                        apiCtx,
                        rscConnApi.getUuid(),
                        sourceResource,
                        targetResource,
                        new ResourceConnection.Flags[]
                        {},
                        null
                    );

                    Props rscConnProps = rscConn.getProps(apiCtx);
                    rscConnProps.map().putAll(rscConnApi.getProps());
                    rscConnProps.keySet().retainAll(rscConnApi.getProps().keySet());

                    rscConn.getStateFlags().resetFlagsTo(
                        apiCtx, ResourceConnection.Flags.restoreFlags(rscConnApi.getFlags())
                    );

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

            layerRscDataMerger.mergeLayerData(localRsc, rscRawData.getLayerData(), false);

            cryptHelper.decryptAllNewLuksVlmKeys(false);

            transMgrProvider.get().commit();

            Set<Resource.ResourceKey> devMgrNotifications = new TreeSet<>();

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

    private void reportSuccess(Set<Resource.ResourceKey> rscSet, String action)
    {
        for (Resource.ResourceKey rscKey : rscSet)
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

    private Resource createRsc(
        UUID rscUuid,
        Node node,
        ResourceDefinition rscDfn,
        Resource.Flags[] flags,
        Map<String, String> rscProps,
        List<VolumeApi> vlms,
        boolean remoteRsc,
        RscLayerDataApi rscLayerDataApi
    )
        throws AccessDeniedException, ValueOutOfRangeException, InvalidNameException, DivergentDataException,
            DatabaseException
    {
        Resource rsc = resourceFactory.getInstanceSatellite(
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

        for (VolumeApi vlmRaw : vlms)
        {
            createVlm(vlmRaw, rsc, remoteRsc);
        }

        return rsc;
    }

    private void createVlm(
        VolumeApi vlmApi,
        Resource rsc,
        boolean remoteRsc
    )
        throws AccessDeniedException, InvalidNameException, DivergentDataException, ValueOutOfRangeException,
        DatabaseException
    {
        VolumeDefinition vlmDfn = rsc.getDefinition().getVolumeDfn(apiCtx, new VolumeNumber(vlmApi.getVlmNr()));

        Volume vlm = volumeFactory.getInstanceSatellite(
            apiCtx,
            vlmApi.getVlmUuid(),
            rsc,
            vlmDfn,
            Volume.Flags.restoreFlags(vlmApi.getFlags())
        );

        vlm.getProps(apiCtx).map().putAll(vlmApi.getVlmProps());

        // XXX check if vlm really has expected layerStack (stack cannot be changed!)
    }

    private void mergeVlm(Volume vlm, VolumeApi vlmApi, boolean remoteRsc)
        throws DivergentDataException, AccessDeniedException, DatabaseException, InvalidNameException
    {
        if (!remoteRsc)
        {
            checkUuid(vlm, vlmApi);
        }

        Props vlmProps = vlm.getProps(apiCtx);
        vlmProps.map().putAll(vlmApi.getVlmProps());
        vlmProps.keySet().retainAll(vlmApi.getVlmProps().keySet());
        vlm.getFlags().resetFlagsTo(apiCtx, Volume.Flags.restoreFlags(vlmApi.getFlags()));
    }

    private void restoreStorPools(Volume vlmRef, VolumeApi vlmApiRef, boolean remoteRscRef)
        throws AccessDeniedException, InvalidNameException, DivergentDataException, DatabaseException
    {
        VolumeNumber vlmNr = vlmRef.getVolumeDefinition().getVolumeNumber();
        Resource rsc = vlmRef.getAbsResource();
        Node node = rsc.getNode();
        NodeName nodeName = node.getName();

        Map<String, VlmProviderObject<Resource>> storVlmObjMap = LayerRscUtils.getRscDataByProvider(
            rsc.getLayerData(apiCtx),
            DeviceLayerKind.STORAGE
        ).stream().collect(Collectors.toMap(
            rscObj -> rscObj.getResourceNameSuffix(),
            rscObj -> rscObj.getVlmProviderObject(vlmNr)
        ));

        for (Pair<String, VlmLayerDataApi> pair : vlmApiRef.getVlmLayerData())
        {
            VlmLayerDataApi vlmApi = pair.objB;
            StorPoolApi storPoolApi = vlmApi.getStorPoolApi();
            if (!storPoolApi.getNodeName().equalsIgnoreCase(nodeName.displayValue))
            {
                throw new DivergentDataException("VlmLayerData from volume " + vlmRef + " contains a reference to " +
                    "a storage pool of a different satellite (" + vlmApi.getStorPoolApi().getNodeName() + ")");
            }

            StorPoolName storPoolName = new StorPoolName(vlmApi.getStorPoolApi().getStorPoolName());
            StorPool storPool = node.getStorPool(apiCtx, storPoolName);
            if (storPool == null)
            {
                if (remoteRscRef)
                {
                    StorPoolDefinition storPoolDfn =
                        storPoolDfnMap.get(new StorPoolName(storPoolApi.getStorPoolName()));
                    if (storPoolDfn == null)
                    {
                        storPoolDfn = storPoolDefinitionFactory.getInstance(
                            apiCtx,
                            storPoolApi.getStorPoolDfnUuid(),
                            new StorPoolName(storPoolApi.getStorPoolName())
                        );
                        storPoolDfn.getProps(apiCtx).map().putAll(storPoolApi.getStorPoolDfnProps());
                        storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);
                    }
                    storPool = storPoolFactory.getInstanceSatellite(
                        apiCtx,
                        storPoolApi.getStorPoolUuid(),
                        rsc.getNode(),
                        storPoolDfn,
                        storPoolApi.getDeviceProviderKind(),
                        freeSpaceMgrFactory.getInstance()
                    );
                    storPool.getProps(apiCtx).map().putAll(storPoolApi.getStorPoolProps());

                }
                else
                {
                    throw new DivergentDataException("Unknown StorPool: '" + storPoolApi.getStorPoolName() + "'");
                }
            }
            if (!remoteRscRef && !storPool.getUuid().equals(storPoolApi.getStorPoolUuid()))
            {
                throw new DivergentUuidsException(
                    "StorPool",
                    storPool.toString(),
                    storPoolApi.getStorPoolName(),
                    storPool.getUuid(),
                    storPoolApi.getStorPoolUuid()
                );
            }

            storVlmObjMap.get(pair.objA).setStorPool(apiCtx, storPool);

            Props storPoolProps = storPool.getProps(apiCtx);
            storPoolProps.map().putAll(storPoolApi.getStorPoolProps());
            storPoolProps.keySet().retainAll(storPoolApi.getStorPoolProps().keySet());




        }
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
                rsc.getNode().getName().displayValue,
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
        VolumeDefinitionApi vlmDfnRaw,
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
        VolumeApi vlmRaw
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
                vlm.getAbsResource().getDefinition().getName().displayValue,
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
