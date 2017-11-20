package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 * Representation of a resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceData extends BaseTransactionObject implements Resource
{
    // Object identifier
    private final UUID objId;

    // Reference to the resource definition
    private final ResourceDefinition resourceDfn;

    // Connections to the peer resources
    private final TransactionMap<Resource, ResourceConnection> resourceConnections;

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

    private final ResourceDataDatabaseDriver dbDriver;

    private boolean deleted = false;

    /*
     * used by getInstance
     */
    private ResourceData(
        AccessContext accCtx,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        NodeId nodeIdRef,
        long initFlags,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            accCtx,
            ObjectProtection.getInstance(
                accCtx,
                ObjectProtection.buildPath(
                    nodeRef.getName(),
                    resDfnRef.getName()
                ),
                true,
                transMgr
            ),
            resDfnRef,
            nodeRef,
            nodeIdRef,
            initFlags,
            transMgr
        );
    }

    /**
     * used by database drivers and tests
     * @throws AccessDeniedException
     */
    ResourceData(
        UUID objIdRef,
        AccessContext accCtx,
        ObjectProtection objProtRef,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        NodeId nodeIdRef,
        long initFlags,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        ErrorCheck.ctorNotNull(ResourceData.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(ResourceData.class, Node.class, nodeRef);
        resNodeId = nodeIdRef;
        resourceDfn = resDfnRef;
        assgNode = nodeRef;
        objId = objIdRef;

        dbDriver = DrbdManage.getResourceDataDatabaseDriver();

        resourceConnections = new TransactionMap<>(new HashMap<Resource, ResourceConnection>(), null);
        volumeMap = new TransactionMap<>(new TreeMap<VolumeNumber, Volume>(), null);
        resourceProps = PropsContainer.getInstance(
            PropsContainer.buildPath(
                nodeRef.getName(),
                resDfnRef.getName()
            ),
            transMgr
        );
        objProt = objProtRef;

        flags = new RscFlagsImpl(objProt, this, dbDriver.getStateFlagPersistence(), initFlags);

        transObjs = Arrays.asList(
            resourceDfn,
            assgNode,
            flags,
            objProt,
            resourceConnections,
            volumeMap,
            resourceProps
        );

        ((NodeData) nodeRef).addResource(accCtx, this);
        ((ResourceDefinitionData) resDfnRef).addResource(accCtx, this);
    }

    public static ResourceData getInstance(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        Node node,
        NodeId nodeId,
        RscFlags[] initFlags,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, DrbdDataAlreadyExistsException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        ResourceData resData = null;

        ResourceDataDatabaseDriver driver = DrbdManage.getResourceDataDatabaseDriver();

        resData = driver.load(node, resDfn.getName(), false, transMgr);

        if (failIfExists && resData != null)
        {
            throw new DrbdDataAlreadyExistsException("The Resource already exists");
        }

        if (resData == null && createIfNotExists)
        {
            resData = new ResourceData(
                accCtx,
                resDfn,
                node,
                nodeId,
                StateFlagsBits.getMask(initFlags),
                transMgr
            );
            driver.create(resData, transMgr);
        }

        if (resData != null)
        {
            resData.initialized();
        }
        return resData;
    }

    public static ResourceData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        ResourceDefinition rscDfn,
        NodeId nodeId,
        RscFlags[] initFlags,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        ResourceDataDatabaseDriver dbDriver = DrbdManage.getResourceDataDatabaseDriver();
        ResourceData rscData;
        try
        {
            rscData = dbDriver.load(node, rscDfn.getName(), false, transMgr);
            if (rscData == null)
            {
                rscData = new ResourceData(
                    uuid,
                    accCtx,
                    ObjectProtection.getInstance(accCtx, "", false, transMgr),
                    rscDfn,
                    node,
                    nodeId,
                    StateFlagsBits.getMask(initFlags),
                    transMgr
                );
            }
            rscData.initialized();
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return rscData;
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
            resourceConnections.put(targetResource, resCon);
        }
        else
        {
            resourceConnections.put(sourceResource, resCon);
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
            resourceConnections.remove(targetResource, con);
        }
        else
        {
            resourceConnections.remove(sourceResource, con);
        }
    }

    @Override
    public ResourceConnection getResourceConnection(AccessContext accCtx, Resource otherResource)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceConnections.get(otherResource);
    }

    @Override
    public Volume getVolume(VolumeNumber volNr)
    {
        checkDeleted();
        return volumeMap.get(volNr);
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
    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        getStateFlags().enableFlags(accCtx, RscFlags.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CONTROL);

        synchronized (assgNode)
        {
            ((NodeData) assgNode).removeResource(accCtx, this);
            synchronized (resourceDfn)
            {
                try
                {
                    ((ResourceDefinitionData) resourceDfn).removeResource(accCtx, this);
                }
                catch (AccessDeniedException accessDeniedExc)
                {
                    ((NodeData) assgNode).addResource(accCtx, this);
                    throw accessDeniedExc;
                }
            }
        }
        dbDriver.delete(this, transMgr);
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted node", null);
        }
    }

    @Override
    public String toString()
    {
        return "Node: '" + assgNode.getName() + "', " +
               "Rsc: '" + resourceDfn.getName() + "'";
    }

    private static final class RscFlagsImpl extends StateFlagsBits<ResourceData, RscFlags>
    {
        RscFlagsImpl(
            ObjectProtection objProtRef,
            ResourceData parent,
            StateFlagsPersistence<ResourceData> persistenceRef,
            long initMask
        )
        {
            super(
                objProtRef,
                parent,
                StateFlagsBits.getMask(RscFlags.values()),
                persistenceRef,
                initMask
            );
        }
    }
}
