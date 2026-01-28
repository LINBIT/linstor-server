package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherNodeNetInterfacePojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherRscPojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.CriticalError;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgrSatelliteFactory;
import com.linbit.linstor.core.objects.NetInterface;
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
import com.linbit.linstor.core.objects.StorPoolDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.StorPoolSatelliteFactory;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.VolumeFactory;
import com.linbit.linstor.core.objects.merger.StltLayerRscDataMerger;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

            ResourceDefinition rscDfn = rscDfnMap.get(rscName);
            if (rscDfn != null)
            {
                // Copy the resources to void concurrentModificationException when deleting the rscs from the rscDfn
                for (Resource rsc : rscDfn.copyResourceMap(apiCtx).values())
                {
                    rsc.delete(apiCtx);
                }
                if (rscDfn.getSnapshotDfns(apiCtx).isEmpty())
                {
                    rscDfnMap.remove(rscName); // just to be sure
                    ResourceGroup rscGrp = rscDfn.getResourceGroup();
                    rscDfn.delete(apiCtx);
                    if (!rscGrp.hasResourceDefinitions(apiCtx))
                    {
                        rscGrpMap.remove(rscGrp.getName());
                        rscGrp.delete(apiCtx);
                    }
                    errorReporter.logInfo("Resource definition '" + rscNameStr +
                        "' and the corresponding resource removed by Controller.");
                }
                else
                {
                    errorReporter.logInfo("Resources of resource definition '" + rscNameStr +
                        "' removed by Controller.");
                }
                transMgrProvider.get().commit();
            }


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

            if (rscDfn == null)
            {
                errorReporter.logTrace("creating new rscDfn '%s'", rscName);
                rscDfn = resourceDefinitionFactory.getInstanceSatellite(
                    apiCtx,
                    rscRawData.getRscDfnUuid(),
                    rscGrp,
                    rscName,
                    rscDfnFlags
                );

                rscDfnToRegister = rscDfn;
            }
            checkUuid(rscDfn, rscRawData);

            ResourceGroup oldRscGrp = rscDfn.getResourceGroup();
            if (!oldRscGrp.equals(rscGrp))
            {
                // setRscGrp also calls oldRscGrp.remove(rscDfn); newRscGrp.add(rscDfn)
                rscDfn.setResourceGroup(apiCtx, rscGrp);
                ResourceGroupName oldRscGrpName = oldRscGrp.getName();
                errorReporter.logInfo(
                    "rscDfn '%s' moved from rscGrp '%s' to '%s'",
                    rscName,
                    oldRscGrpName.displayValue,
                    rscGrp.getName().displayValue
                );
                if (oldRscGrp.getRscDfns(apiCtx).isEmpty())
                {
                    errorReporter.logTrace("deleting no longer used rscGrp '%s'", oldRscGrpName.displayValue);
                    oldRscGrp.delete(apiCtx);
                    rscGrpMap.remove(oldRscGrpName);
                }
            }

            Props rscDfnProps = rscDfn.getProps(apiCtx);
            rscDfnProps.map().putAll(rscRawData.getRscDfnProps());
            rscDfnProps.keySet().retainAll(rscRawData.getRscDfnProps().keySet());
            errorReporter.logTrace(
                "resetting flags of local rscdfn (%s) to %s",
                rscDfn,
                FlagsHelper.toStringList(ResourceDefinition.Flags.class, rscRawData.getRscDfnFlags())
            );
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

                    vlmDfn.getFlags().resetFlagsTo(apiCtx, vlmDfnFlags);

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

            final Set<Resource.ResourceKey> createdRscSet = new TreeSet<>();
            final Set<Resource.ResourceKey> updatedRscSet = new TreeSet<>();
            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            Resource localRsc = null;
            if (!rscIterator.hasNext())
            {
                // our rscDfn is empty
                // that means, just create everything we need

                Node localNode = controllerPeerConnector.getLocalNode();

                localRsc = createRsc(
                    rscRawData.getUuid(),
                    localNode,
                    rscDfn,
                    Resource.Flags.restoreFlags(rscRawData.getFlags()),
                    rscRawData.getProps(),
                    (List<VolumeApi>) rscRawData.getVlmList(),
                    false,
                    rscRawData.getLayerData()
                );

                errorReporter.logTrace(
                    "%s created with flags %s",
                    localRsc,
                    FlagsHelper.toStringList(Resource.Flags.class, rscRawData.getFlags())
                );

                createdRscSet.add(new Resource.ResourceKey(localRsc));

                for (OtherRscPojo otherRscRaw : rscRawData.getOtherRscList())
                {
                    Node remoteNode = getMergedRemoteNode(otherRscRaw);

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

                    errorReporter.logTrace(
                        "%s created with flags %s",
                        remoteRsc,
                        FlagsHelper.toStringList(Resource.Flags.class, otherRscRaw.getRscFlags())
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
                    if (rsc.getUuid().equals(rscRawData.getUuid()))
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
                    CriticalError.dieSoon(
                        String.format(
                            "The local resource with the UUID '%s' was not found in the stored " +
                                "resource definition '%s'.",
                            rscRawData.getUuid().toString(),
                            rscName)
                    );
                }

                // update volumes
                {

                    // we do not have to care about deletion, as the merge of vlmDfns should have already marked
                    // all the corresponding volumes for deletion
                    for (VolumeApi vlmApi : rscRawData.getVlmList())
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
                    localRscProps.map().putAll(rscRawData.getProps());
                    localRscProps.keySet().retainAll(rscRawData.getProps().keySet());
                }

                // update flags
                errorReporter.logTrace(
                    "resetting flags of local rsc (%s) to %s",
                    localRsc,
                    FlagsHelper.toStringList(Resource.Flags.class, rscRawData.getFlags())
                );
                localRsc.getStateFlags().resetFlagsTo(
                    apiCtx,
                    Resource.Flags.restoreFlags(rscRawData.getFlags())
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
                                CriticalError.dieSoon(
                                    "The resource with UUID '%s' was deployed on node '%s' but is now " +
                                        "on node '%s' (this should have cause a delete and re-deploy of that resource)."
                                );
                            }
                            if (!otherRsc.getNodeUuid().equals(
                                removed.getNode().getUuid())
                            )
                            {
                                CriticalError.dieUuidMissmatch(
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
                        Node remoteNode = getMergedRemoteNode(otherRsc);

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
                        // we found the resource by the uuid the controller sent us, but we still need to make sure
                        // to merge the remoteNode in case some properties / netIfs have changed.
                        getMergedRemoteNode(otherRsc);

                        // update matching resource props
                        Props remoteRscProps = remoteRsc.getProps(apiCtx);
                        remoteRscProps.map().putAll(otherRsc.getRscProps());
                        remoteRscProps.keySet().retainAll(otherRsc.getRscProps().keySet());

                        errorReporter.logTrace(
                            "resetting flags of remote rsc (%s) to %s",
                            remoteRsc,
                            FlagsHelper.toStringList(Resource.Flags.class, otherRsc.getRscFlags())
                        );
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
                        null,
                        null
                    );

                    Props rscConnProps = rscConn.getProps(apiCtx);
                    rscConnProps.map().putAll(rscConnApi.getProps());
                    rscConnProps.keySet().retainAll(rscConnApi.getProps().keySet());

                    rscConn.getStateFlags().resetFlagsTo(
                        apiCtx, ResourceConnection.Flags.restoreFlags(rscConnApi.getFlags())
                    );

                    rscConn.setDrbdProxyPortSource(
                        apiCtx,
                        rscConnApi.getDrbdProxyPortSource() == null ?
                            null :
                            new TcpPortNumber(rscConnApi.getDrbdProxyPortSource())
                    );
                    rscConn.setDrbdProxyPortTarget(
                        apiCtx,
                        rscConnApi.getDrbdProxyPortTarget() == null ?
                            null :
                            new TcpPortNumber(rscConnApi.getDrbdProxyPortTarget())
                    );

                }
            }

            if (rscDfnToRegister != null)
            {
                rscDfnMap.put(rscName, rscDfnToRegister);
            }

            layerRscDataMerger.mergeLayerData(localRsc, rscRawData.getLayerData(), false);

            cryptHelper.decryptVolumesAndDrives(false);

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

    /**
     * Gets the remote {@link Node} and merges the nodes properties and network interfaces based on the content of
     * <code>otherRscRawRef</code>
     *
     * @param otherRscRawRef
     * @return The fully merged remote {@link Node}
     */
    private Node getMergedRemoteNode(OtherRscPojo otherRscRawRef)
        throws ImplementationError, InvalidNameException, AccessDeniedException, InvalidIpAddressException,
        DatabaseException
    {
        Node remoteNode = nodeFactory.getInstanceSatellite(
            apiCtx,
            otherRscRawRef.getNodeUuid(),
            new NodeName(otherRscRawRef.getNodeName()),
            Node.Type.valueOf(otherRscRawRef.getNodeType()),
            Node.Flags.restoreFlags(otherRscRawRef.getNodeFlags())
        );
        checkUuid(remoteNode, otherRscRawRef);
        Map<String, String> propsMap = remoteNode.getProps(apiCtx).map();
        propsMap.clear();
        propsMap.putAll(otherRscRawRef.getNodeProps());

        mergeNetIfs(remoteNode, otherRscRawRef);

        return remoteNode;
    }

    private void mergeNetIfs(Node remoteNode, OtherRscPojo otherRscRawRef)
        throws AccessDeniedException, InvalidNameException, InvalidIpAddressException, ImplementationError,
        DatabaseException
    {
        // the list of netIfs could be empty if we are processing (remote) resources during a FullSync. The FullSync
        // only includes remote netIfs in the IntNode proto messages, but not in the remote part of IntRsc. So the
        // FullSync will correctly apply the remote netIfs when processing the remote nodes, but once we process the
        // remote rscs (here) we see the empty list and delete all remote netIfs.

        // during a non-FullSync (i.e. an update) the remote node's netIfs are included and the list of remote netIfs
        // is not empty.

        // also in general it makes no sense to "force-apply" an empty list of netIfs. We only know remote nodes if we
        // have a shared resource with them. Shared resource without a netIf does not work.
        if (!otherRscRawRef.getNetInterfacefPojos().isEmpty())
        {
            // since we should also to delete no longer existing netIfs, we first create a list of registered netIfs of
            // the remoteNode
            Map<NetInterfaceName, NetInterface> netIfsToRemove = new HashMap<>();
            Iterator<NetInterface> netIfIt = remoteNode.iterateNetInterfaces(apiCtx);
            while (netIfIt.hasNext())
            {
                NetInterface netIf = netIfIt.next();
                netIfsToRemove.put(netIf.getName(), netIf);
            }

            // set node's netinterfaces
            for (OtherNodeNetInterfacePojo otherNodeNetIf : otherRscRawRef.getNetInterfacefPojos())
            {
                NetInterfaceName netIfName = new NetInterfaceName(otherNodeNetIf.getName());
                LsIpAddress addr = new LsIpAddress(otherNodeNetIf.getAddress());
                NetInterface netIf = netInterfaceFactory.getInstanceSatellite(
                    apiCtx,
                    otherNodeNetIf.getUuid(),
                    remoteNode,
                    netIfName,
                    addr
                );
                if (!netIf.getAddress(apiCtx).equals(addr))
                {
                    netIf.setAddress(apiCtx, addr);
                }
                netIfsToRemove.remove(netIfName);
            }

            // remove nodes no longer known netIfs
            for (NetInterface netIf : netIfsToRemove.values())
            {
                remoteNode.removeNetInterface(apiCtx, netIf);
            }
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

            errorReporter.logDebug(msgBuilder.toString());
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
        throws AccessDeniedException, ValueOutOfRangeException, InvalidNameException,
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
        throws AccessDeniedException, InvalidNameException, ValueOutOfRangeException,
        DatabaseException
    {
        VolumeDefinition vlmDfn = rsc.getResourceDefinition().getVolumeDfn(apiCtx, new VolumeNumber(vlmApi.getVlmNr()));

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
        throws AccessDeniedException, DatabaseException, InvalidNameException
    {
        if (!remoteRsc)
        {
            checkUuid(vlm, vlmApi);
        }

        Props vlmProps = vlm.getProps(apiCtx);
        vlmProps.map().putAll(vlmApi.getVlmProps());
        vlmProps.keySet().retainAll(vlmApi.getVlmProps().keySet());
        errorReporter.logTrace(
            "resetting flags of %s vlm (%s) to %s",
            remoteRsc ? "remote" : "local",
            vlm.toString(),
            FlagsHelper.toStringList(Volume.Flags.class, vlmApi.getFlags())
        );
        vlm.getFlags().resetFlagsTo(apiCtx, Volume.Flags.restoreFlags(vlmApi.getFlags()));
    }

    private void checkUuid(Node node, OtherRscPojo otherRsc)
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
    {
        checkUuid(
            rscDfn.getUuid(),
            rscRawData.getRscDfnUuid(),
            "ResourceDefinition",
            rscDfn.getName().displayValue,
            rscRawData.getName()
        );
    }

    private void checkUuid(
        VolumeDefinition vlmDfn,
        VolumeDefinitionApi vlmDfnRaw,
        String remoteRscName
    )
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
    {
        checkUuid(
            vlm.getUuid(),
            vlmRaw.getVlmUuid(),
            "Volume",
            vlm.getKey().toString(),
            String.format(
                "Rsc: '%s', VlmNr: '%d'",
                vlm.getAbsResource().getResourceDefinition().getName().displayValue,
                vlmRaw.getVlmNr()
            )
        );
    }

    private void checkUuid(UUID localUuid, UUID remoteUuid, String type, String localName, String remoteName)
    {
        if (!localUuid.equals(remoteUuid))
        {
            CriticalError.dieUuidMissmatch(
                type,
                localName,
                remoteName,
                localUuid,
                remoteUuid
            );
        }
    }
}
