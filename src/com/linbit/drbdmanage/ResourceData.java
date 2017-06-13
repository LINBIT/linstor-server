package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Representation of a resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceData implements Resource
{
    // Object identifier
    private UUID objId;

    // Reference to the resource definition
    private ResourceDefinition resourceDfn;

    // List of volumes of this resource
    private TransactionMap<VolumeNumber, Volume> volumeList;

    // Reference to the node this resource is assigned to
    private Node assgNode;

    // State flags
    private StateFlags<RscFlags> flags;

    // Access control for this resource
    private ObjectProtection objProt;

    // DRBD node id for this resource
    private NodeId resNodeId;

    // Properties container for this resource
    private Props resourceProps;

    private ResourceDatabaseDriver dbDriver;

    private ResourceData(
        AccessContext accCtx,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        NodeId nodeIdRef,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        resNodeId = nodeIdRef;
        ErrorCheck.ctorNotNull(ResourceData.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(ResourceData.class, Node.class, nodeRef);
        resourceDfn = resDfnRef;
        assgNode = nodeRef;
        objId = UUID.randomUUID();

        dbDriver = DrbdManage.getResourceDatabaseDriver(this);

        volumeList = new TransactionMap<>(
            new TreeMap<VolumeNumber, Volume>(),
            dbDriver.getVolumeMapDriver()
        );
        resourceProps = SerialPropsContainer.createRootContainer(srlGen);
        objProt = ObjectProtection.load(
            transMgr,
            ObjectProtection.buildPath(this),
            true,
            accCtx
        );
        flags = new RscFlagsImpl(objProt, dbDriver.getStateFlagPersistence());
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        return PropsAccess.secureGetProps(accCtx, objProt, resourceProps);
    }

    @Override
    public ResourceDefinition getDefinition()
    {
        return resourceDfn;
    }

    @Override
    public Volume getVolume(VolumeNumber volNr)
    {
        return volumeList.get(volNr);
    }

    @Override
    public Iterator<Volume> iterateVolumes()
    {
        return Collections.unmodifiableCollection(volumeList.values()).iterator();
    }

    @Override
    public Node getAssignedNode()
    {
        return assgNode;
    }

    @Override
    public NodeId getNodeId()
    {
        return resNodeId;
    }

    @Override
    public StateFlags<RscFlags> getStateFlags()
    {
        return flags;
    }

    public static Resource create(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        Node node,
        NodeId nodeId,
        SerialGenerator srlGen
    )
        throws AccessDeniedException, SQLException
    {
        return create(accCtx, resDfn, node, nodeId, srlGen, null);
    }

    public static Resource create(
        AccessContext accCtx,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        NodeId nodeId,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws AccessDeniedException, SQLException
    {
        ErrorCheck.ctorNotNull(Resource.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(Resource.class, Node.class, nodeRef);

        Resource newRes = new ResourceData(accCtx, resDfnRef, nodeRef, nodeId, srlGen, transMgr);

        // Access controls on the node and resource must not change
        // while the transaction is in progress
        synchronized (nodeRef)
        {
            synchronized (resDfnRef)
            {
                nodeRef.addResource(accCtx, newRes);
                try
                {
                    resDfnRef.addResource(accCtx, newRes);
                }
                catch (AccessDeniedException accExc)
                {
                    // Rollback adding the resource to the node
                    nodeRef.removeResource(accCtx, newRes);
                }
            }
        }

        return newRes;
    }

    // TODO: implement ResourceData.load(...)

    private static final class RscFlagsImpl extends StateFlagsBits<RscFlags>
    {
        RscFlagsImpl(ObjectProtection objProtRef, StateFlagsPersistence persistenceRef)
        {
            super(objProtRef, StateFlagsBits.getMask(RscFlags.ALL_FLAGS), persistenceRef);
        }
    }

    @Override
    public void setConnection(TransactionMgr transMgr) throws ImplementationError
    {
        transMgr.register(this);
        dbDriver.setConnection(transMgr.dbCon);
    }

    @Override
    public void commit()
    {
        resourceDfn.commit();
        volumeList.commit();
        assgNode.commit();
        flags.commit();
        objProt.commit();
        resourceProps.commit();
    }

    @Override
    public void rollback()
    {
        resourceDfn.rollback();
        volumeList.rollback();
        assgNode.rollback();
        flags.rollback();
        objProt.rollback();
        resourceProps.rollback();
    }

    @Override
    public boolean isDirty()
    {
        return resourceDfn.isDirty() ||
            volumeList.isDirty()     ||
            assgNode.isDirty()       ||
            flags.isDirty()          ||
            objProt.isDirty()        ||
            resourceProps.isDirty();
    }
}
