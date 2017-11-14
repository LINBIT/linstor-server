package com.linbit.drbdmanage.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.Node.NodeFlag;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.NodeId;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceDefinition.RscDfnFlags;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.TcpPortNumber;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnFlags;
import com.linbit.drbdmanage.api.raw.ResourceRawData;
import com.linbit.drbdmanage.api.raw.ResourceRawData.OtherRscRawData;
import com.linbit.drbdmanage.api.raw.ResourceRawData.VolumeDfnRawData;
import com.linbit.drbdmanage.api.raw.ResourceRawData.VolumeRawData;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

public class StltRscApiCallHandler
{
    private final Satellite satellite;
    private final AccessContext apiCtx;

    public StltRscApiCallHandler(Satellite satelliteRef, AccessContext apiCtxRef)
    {
        satellite = satelliteRef;
        apiCtx = apiCtxRef;
    }

    public void deployResource(ResourceRawData rscRawData)
        throws DivergentDataException
    {
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        ResourceName rscName;

        ResourceDefinitionData rscDfnToRegister = null;
        List<NodeData> nodesToRegister = new ArrayList<>();

        try
        {
            rscName = new ResourceName(rscRawData.getRscName());
            TcpPortNumber port = new TcpPortNumber(rscRawData.getRscDfnPort());
            RscDfnFlags[] rscDfnFlags = RscDfnFlags.restoreFlags(rscRawData.getRscDfnFlags());

            ResourceDefinitionData rscDfn = (ResourceDefinitionData) satellite.rscDfnMap.get(rscName);

            if (rscDfn == null)
            {
                rscDfn = ResourceDefinitionData.getInstanceSatellite(
                    apiCtx,
                    rscRawData.getRscDfnUuid(),
                    rscName,
                    port,
                    rscDfnFlags,
                    transMgr
                );

                checkUuid(rscDfn, rscRawData);
                rscDfnToRegister = rscDfn;
            }
            rscDfn.setConnection(transMgr);
            // "restoring" rscDfn data (overriding whatever it had before, even if we just created it)
            rscDfn.setPort(apiCtx, port);
            Map<String, String> rscDfnProps = rscDfn.getProps(apiCtx).map();
            rscDfnProps.clear();
            rscDfnProps.putAll(rscRawData.getRscDfnProps());
            rscDfn.getFlags().resetFlagsTo(apiCtx, rscDfnFlags);

            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            if (!rscIterator.hasNext())
            {
                // our rscDfn is empty
                // that means, just create everything we need

                NodeData localNode = satellite.getLocalNode();
                NodeId localNodeId = new NodeId(rscRawData.getLocalRscNodeId());
                RscFlags[] localRscFlags = RscFlags.restoreFlags(rscRawData.getLocalRscFlags());
                ResourceData rsc = ResourceData.getInstanceSatellite(
                    apiCtx,
                    rscRawData.getLocalRscUuid(),
                    localNode,
                    rscDfn,
                    localNodeId,
                    localRscFlags,
                    transMgr
                );
                rsc.getProps(apiCtx).map().putAll(rscRawData.getLocalRscProps());

                Map<VolumeNumber, VolumeDefinitionData> vlmDfns = new HashMap<>();

                for (VolumeDfnRawData vlmDfnRaw : rscRawData.getVlmDfns())
                {
                    VolumeNumber vlmNr = new VolumeNumber(vlmDfnRaw.getVlmNr());
                    VolumeDefinitionData vlmDfn = VolumeDefinitionData.getInstanceSatellite(
                        apiCtx,
                        vlmDfnRaw.getVlmDfnUuid(),
                        rscDfn,
                        vlmNr,
                        vlmDfnRaw.getVlmSize(),
                        new MinorNumber(vlmDfnRaw.getVlmMinor()),
                        VlmDfnFlags.restoreFlags(vlmDfnRaw.getFlags()),
                        transMgr
                    );
                    checkUuid(vlmDfn, vlmDfnRaw, rscName.displayValue);
                    Map<String, String> vlmDfnPropsMap = vlmDfn.getProps(apiCtx).map();
                    vlmDfnPropsMap.clear();
                    vlmDfnPropsMap.putAll(vlmDfnRaw.getVlmDfnProps());

                    vlmDfns.put(vlmNr, vlmDfn);
                }

                for (VolumeRawData vlmRaw : rscRawData.getLocalVlms())
                {
                    StorPool storPool = localNode.getStorPool(
                        apiCtx,
                        new StorPoolName(vlmRaw.getStorPoolName())
                    );
                    if (storPool == null)
                    {
                        throw new DivergentDataException("Unknown StorPool: '" + vlmRaw.getStorPoolName() + "'");
                    }
                    if (!storPool.getUuid().equals(vlmRaw.getStorPoolUuid()))
                    {
                        throw new DivergentUuidsException(
                            "StorPool",
                            storPool.toString(),
                            vlmRaw.getStorPoolName(),
                            storPool.getUuid(),
                            vlmRaw.getStorPoolUuid()
                        );
                    }

                    VolumeData vlm = VolumeData.getInstanceSatellite(
                        apiCtx,
                        vlmRaw.getVlmUuid(),
                        rsc,
                        vlmDfns.get(new VolumeNumber(vlmRaw.getVlmNr())),
                        storPool,
                        vlmRaw.getBlockDevice(),
                        vlmRaw.getMetaDisk(),
                        VlmFlags.restoreFlags(vlmRaw.getVlmFlags()),
                        transMgr
                    );

                    checkUuid(vlm, vlmRaw, localNode.getName().displayValue, rscName.displayValue);
                }
            }
            else
            {
                // iterator contains at least one resource.
                // in this case, one of the resources has to be our local resource
                // because if we had undeployed our local resource with the rscDfn's name, we should
                // have also removed the rscDfn from satellite.rscDfnMap (as we would no longer need it)
                // in that case the rscDfn.getInstanceSatellite would not find the rscDfn,
                // therefore creating a new rscDfn with no resources,
                // thus we should be on the "then" branch instead of this "else" branch.

                Resource localRsc = null;
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
                    rscIterator.remove();
                }

                // now we should have found our localRsc, and marked all other resources for deletion
                // we will "unmark" them as we find them with matching uuid and node names
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
                for (OtherRscRawData otherRsc : rscRawData.getOtherRscList())
                {
                    Resource match = null;
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

                            match = removed;
                            break;
                        }
                    }
                    if (match == null)
                    {
                        // controller sent us a resource that we don't know
                        // create its node
                        NodeName otherNodeName = new NodeName(otherRsc.getNodeName());
                        NodeType nodeType = NodeType.getByValue(otherRsc.getNodeType());
                        NodeFlag[] nodeFlags = NodeFlag.restoreFlags(otherRsc.getNodeFlags());
                        NodeData otherNode = NodeData.getInstanceSatellite(
                            apiCtx,
                            otherRsc.getNodeUuid(),
                            otherNodeName,
                            nodeType,
                            nodeFlags,
                            transMgr
                        );
                        nodesToRegister.add(otherNode);

                        // as a node with that name could already exist, check the uuid
                        checkUuid(otherNode, otherRsc);

                        NodeId otherNodeId = new NodeId(otherRsc.getRscNodeId());
                        RscFlags[] rscFlags = RscFlags.restoreFlags(otherRsc.getRscFlags());
                        // create resource
                        match = ResourceData.getInstanceSatellite(
                            apiCtx,
                            otherRsc.getRscUuid(),
                            otherNode,
                            rscDfn,
                            otherNodeId,
                            rscFlags,
                            transMgr
                        );
                        // we prior searched for a resource matching our uuid
                        // however, it is possible that a known node already contains a resource with this name
                        // but then, the uuids should mismatch.
                        checkUuid(match, otherRsc, rscRawData.getRscName());

                        // TODO: volumes

                        // everything ok, mark the resource as new
                        newResources.add(match);
                    }
                    else
                    {
                        // we found the resource by the uuid the controller sent us
                        Node otherNode = match.getAssignedNode();
                        // check if the node uuids also match
                        checkUuid(otherNode, otherRsc);

                        // update node props
                        otherNode.setConnection(transMgr);
                        Map<String, String> otherNodeProps = otherNode.getProps(apiCtx).map();
                        otherNodeProps.clear();
                        otherNodeProps.putAll(otherRsc.getNodeProps());

                        // update matching resource props
                        match.setConnection(transMgr);
                        Map<String, String> otherRscProps = match.getProps(apiCtx).map();
                        otherRscProps.clear();
                        otherRscProps.putAll(otherRsc.getRscProps());

                        // TODO: volumes
                        List<VolumeRawData> otherRscVlms = otherRsc.getVlms();

                        // everything ok, mark the resource to be kept
                        removedList.remove(match);
                        modifiedResources.add(match);
                    }
                }
                // all resources have been created or updated

                // cleanup

                // first, iterate over all resources marked for deletion and unlink them from rscDfn and node
                for (Resource rsc : removedList)
                {
                    // TODO: start undeploying this resource on drbd level
                    // TODO: when undeploy is done, check the node if it has resources left. if not, remove node
                    rsc.markDeleted(apiCtx);
                }
            }

            if (rscDfnToRegister != null)
            {
                satellite.rscDfnMap.put(rscName, rscDfnToRegister);
            }
            for (Node node : nodesToRegister)
            {
                satellite.nodesMap.put(node.getName(), node);
            }
            transMgr.commit();

            // TODO: deploy to drbd
        }
        catch (InvalidNameException | ValueOutOfRangeException | NullPointerException invalidDataExc)
        {
            satellite.getErrorReporter().reportError(
                new ImplementationError(
                    "Controller sent invalid Data for resource deployment",
                    invalidDataExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            satellite.getErrorReporter().reportError(
                new ImplementationError(
                    "Satellite's apiContext has not enough privileges to deploy resource",
                    accDeniedExc
                )
            );
        }
        catch (SQLException sqlExc)
        {
            satellite.getErrorReporter().reportError(
                new ImplementationError(
                    "Satellite should not throw this exception.",
                    sqlExc
                )
            );
        }
    }

    private void checkUuid(Node node, OtherRscRawData otherRsc)
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

    private void checkUuid(ResourceDefinition rscDfn, ResourceRawData rscRawData)
        throws DivergentUuidsException
    {
        checkUuid(
            rscDfn.getUuid(),
            rscRawData.getRscDfnUuid(),
            "ResourceDefinition",
            rscDfn.getName().displayValue,
            rscRawData.getRscName()
        );
    }

    private void checkUuid(Resource rsc, OtherRscRawData otherRsc, String otherRscName)
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
        VolumeDfnRawData vlmDfnRaw,
        String remoteRscName
    )
        throws DivergentUuidsException
    {
        checkUuid(
            vlmDfn.getUuid(),
            vlmDfnRaw.getVlmDfnUuid(),
            "VolumeDefinition",
            vlmDfn.toString(),
            String.format(
                "Rsc: '%s', VlmNr: '%d'",
                remoteRscName,
                vlmDfnRaw.getVlmNr()
            )
        );
    }

    private void checkUuid(
        VolumeData vlm,
        VolumeRawData vlmRaw,
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
}
