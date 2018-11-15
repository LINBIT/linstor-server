package com.linbit.linstor;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage2.layer.data.categories.RscLayerData;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Provider;

/**
 * Representation of a resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceData extends BaseTransactionObject implements Resource
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Reference to the resource definition
    private final ResourceDefinition resourceDfn;

    // Connections to the peer resources
    private final TransactionMap<Resource.Key, ResourceConnection> resourceConnections;

    // List of volumes of this resource
    private final TransactionMap<VolumeNumber, Volume> volumeMap;

    // Reference to the node this resource is assigned to
    private final Node assgNode;

    // State flags
    private final StateFlags<RscFlags> flags;

    // Access control for this resource
    private final ObjectProtection objProt;

    // DRBD node id for this resource
    private final NodeId resNodeId;

    // Properties container for this resource
    private final Props resourceProps;

    private final ResourceType rscType;

    private final ResourceDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<ResourceData, Boolean> deleted;

    private boolean createPrimary = false;

    private final Key rscKey;

    private final TransactionList<ResourceData, Resource> children;

    private final TransactionSimpleObject<ResourceData, RscLayerData> layerData;

    private final TransactionSimpleObject<ResourceData, Resource> parent;

    ResourceData(
        UUID objIdRef,
        ObjectProtection objProtRef,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        NodeId nodeIdRef,
        long initFlags,
        ResourceType rscTypeRef,
        List<Resource> childrenRef,
        ResourceDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<Resource.Key, ResourceConnection> rscConnMapRef,
        Map<VolumeNumber, Volume> vlmMapRef,
        RscLayerData layerDataRef
    )
        throws SQLException
    {
        super(transMgrProviderRef);
        dbDriver = dbDriverRef;

        ErrorCheck.ctorNotNull(ResourceData.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(ResourceData.class, Node.class, nodeRef);
        resNodeId = nodeIdRef;
        resourceDfn = resDfnRef;
        assgNode = nodeRef;
        objId = objIdRef;
        rscType = rscTypeRef;
        dbgInstanceId = UUID.randomUUID();

        resourceConnections = transObjFactory.createTransactionMap(rscConnMapRef, null);
        volumeMap = transObjFactory.createTransactionMap(vlmMapRef, null);
        resourceProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                nodeRef.getName(),
                resDfnRef.getName()
            )
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);
        objProt = objProtRef;

        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            RscFlags.class,
            dbDriver.getStateFlagPersistence(),
            initFlags
        );

        rscKey = new Key(this);

        children = transObjFactory.createTransactionList(
            this,
            childrenRef,
            null // TODO: create actual database driver
        );
        parent = transObjFactory.createTransactionSimpleObject(
            this,
            null,
            null // TODO: create actual database driver
        );
        layerData = transObjFactory.createTransactionSimpleObject(this, layerDataRef, null);
        // TODO: create actual database driver

        transObjs = Arrays.asList(
            resourceDfn,
            assgNode,
            flags,
            objProt,
            resourceConnections,
            volumeMap,
            resourceProps,
            children,
            layerData,
            deleted
        );
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, resourceProps);
    }

    @Override
    public ResourceDefinition getDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    @Override
    public synchronized void setResourceConnection(AccessContext accCtx, ResourceConnection resCon)
        throws AccessDeniedException
    {
        checkDeleted();

        Resource sourceResource = resCon.getSourceResource(accCtx);
        Resource targetResource = resCon.getTargetResource(accCtx);

        sourceResource.getObjProt().requireAccess(accCtx, AccessType.USE);
        targetResource.getObjProt().requireAccess(accCtx, AccessType.USE);

        if (this == sourceResource)
        {
            resourceConnections.put(targetResource.getKey(), resCon);
        }
        else
        {
            resourceConnections.put(sourceResource.getKey(), resCon);
        }
    }

    @Override
    public synchronized void removeResourceConnection(AccessContext accCtx, ResourceConnection con)
        throws AccessDeniedException
    {
        checkDeleted();
        Resource sourceResource = con.getSourceResource(accCtx);
        Resource targetResource = con.getTargetResource(accCtx);

        sourceResource.getObjProt().requireAccess(accCtx, AccessType.USE);
        targetResource.getObjProt().requireAccess(accCtx, AccessType.USE);

        if (this == sourceResource)
        {
            resourceConnections.remove(targetResource.getKey());
        }
        else
        {
            resourceConnections.remove(sourceResource.getKey());
        }
    }

    @Override
    public Stream<ResourceConnection> streamResourceConnections(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceConnections.values().stream();
    }

    @Override
    public ResourceConnection getResourceConnection(AccessContext accCtx, Resource otherResource)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceConnections.get(otherResource.getKey());
    }

    @Override
    public Volume getVolume(VolumeNumber volNr)
    {
        checkDeleted();
        return volumeMap.get(volNr);
    }

    @Override
    public int getVolumeCount()
    {
        checkDeleted();
        return volumeMap.size();
    }

    synchronized Volume putVolume(AccessContext accCtx, Volume vol)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        return volumeMap.put(vol.getVolumeDefinition().getVolumeNumber(), vol);
    }

    synchronized void removeVolume(AccessContext accCtx, Volume vol)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        volumeMap.remove(vol.getVolumeDefinition().getVolumeNumber());
    }

    @Override
    public Iterator<Volume> iterateVolumes()
    {
        checkDeleted();
        return Collections.unmodifiableCollection(volumeMap.values()).iterator();
    }

    @Override
    public Stream<Volume> streamVolumes()
    {
        checkDeleted();
        return Collections.unmodifiableCollection(volumeMap.values()).stream();
    }

    @Override
    public Node getAssignedNode()
    {
        checkDeleted();
        return assgNode;
    }

    @Override
    public NodeId getNodeId()
    {
        checkDeleted();
        return resNodeId;
    }

    @Override
    public StateFlags<RscFlags> getStateFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public boolean disklessForPeers(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return flags.isSet(accCtx, RscFlags.DISKLESS) &&
            flags.isUnset(accCtx, RscFlags.DISK_ADDING) &&
            flags.isUnset(accCtx, RscFlags.DISK_REMOVING);
    }

    @Override
    public ResourceType getType()
    {
        return rscType;
    }

    @Override
    public RscLayerData setLayerData(AccessContext accCtx, RscLayerData rscLayerData)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        RscLayerData ret = layerData.get();
        layerData.set(rscLayerData);
        return ret;
    }

    @Override
    public RscLayerData getLayerData(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        return layerData.get();
    }

    @Override
    public List<Resource> getChildResources(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        return children;
    }

    @Override
    public Resource getParentResource(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return parent.get();
    }

    @Override
    public void setParentResource(AccessContext accCtx, Resource parentRef, boolean overrideOldParent)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        Resource parentRsc = parent.get();
        if (parentRsc == null || overrideOldParent)
        {
            if (parentRsc != null)
            {
                parentRsc.removeChild(accCtx, this);
            }
            parent.set(parentRef);
            parentRef.addChild(accCtx, this);

            // check for cycles

            parentRsc = parentRef;
            while (parentRsc != null)
            {
                if (parentRsc.equals(this))
                {
                    throw new ImplementationError("Cyclic resource parent-child relation detected");
                }
                parentRsc = parentRsc.getParentResource(accCtx);
            }
        }
        else
        {
            throw new ImplementationError("Unintentional override of parent-resource");
        }
    }

    @Override
    public void removeParent(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        parent.get().removeChild(accCtx, this);
        parent.set(null);
    }

    @Override
    public void addChild(AccessContext accCtx, Resource child) throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        children.add(child);
    }

    @Override
    public void removeChild(AccessContext accCtx, Resource child) throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        children.remove(child);
    }

    @Override
    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        getStateFlags().enableFlags(accCtx, RscFlags.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            ((NodeData) assgNode).removeResource(accCtx, this);
            ((ResourceDefinitionData) resourceDfn).removeResource(accCtx, this);

            // preventing ConcurrentModificationException
            Collection<ResourceConnection> rscConnValues = new ArrayList<>(resourceConnections.values());
            for (ResourceConnection rscConn : rscConnValues)
            {
                rscConn.delete(accCtx);
            }

            // preventing ConcurrentModificationException
            Collection<Volume> vlmValues = new ArrayList<>(volumeMap.values());
            for (Volume vlm : vlmValues)
            {
                vlm.delete(accCtx);
            }

            resourceProps.delete();

            objProt.delete(accCtx);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public boolean isDeleted()
    {
        return deleted.get();
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted resource");
        }
    }

    public void setCreatePrimary()
    {
        createPrimary = true;
    }

    public void unsetCreatePrimary()
    {
        createPrimary = false;
    }

    @Override
    public boolean isCreatePrimary()
    {
        return createPrimary;
    }

    @Override
    public boolean isDiskless(AccessContext accCtx) throws AccessDeniedException
    {
        return getStateFlags().isSet(accCtx, RscFlags.DISKLESS);
    }

    @Override
    public boolean supportsDrbd(AccessContext accCtx)
        throws AccessDeniedException
    {
        boolean supportsDrbd = true;
        for (Volume vlm : volumeMap.values())
        {
            if (!vlm.getStorPool(accCtx).getDriverKind().supportsDrbd())
            {
                supportsDrbd = false;
            }
        }
        return supportsDrbd;
    }

    @Override
    public RscApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException
    {
        List<Volume.VlmApi> volumes = new ArrayList<>();
        Iterator<Volume> itVolumes = iterateVolumes();
        while (itVolumes.hasNext())
        {
            volumes.add(itVolumes.next().getApiData(null, accCtx));
        }
        List<ResourceConnection.RscConnApi> rscConns = new ArrayList<>();
        for (ResourceConnection rscConn : streamResourceConnections(accCtx).collect(Collectors.toList()))
        {
            rscConns.add(rscConn.getApiData(accCtx));
        }
        return new RscPojo(
            getDefinition().getName().getDisplayName(),
            getAssignedNode().getName().getDisplayName(),
            getAssignedNode().getUuid(),
            getDefinition().getApiData(accCtx),
            getUuid(),
            getStateFlags().getFlagsBits(accCtx),
            getNodeId().value,
            getProps(accCtx).map(),
            volumes,
            null, // otherRscList
            rscConns,
            fullSyncId,
            updateId
        );
    }

    @Override
    public Key getKey()
    {
        // no call to checkDeleted
        return rscKey;
    }

    @Override
    public String toString()
    {
        return "Node: '" + assgNode.getName() + "', " +
               "Rsc: '" + resourceDfn.getName() + "', " +
               "Type: '" + rscType.name() + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }
}
