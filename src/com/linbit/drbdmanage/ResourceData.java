package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.sql.SQLException;
import java.util.Arrays;
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
public class ResourceData extends BaseTransactionObject implements Resource
{
    // Object identifier
    private UUID objId;

    // Reference to the resource definition
    private ResourceDefinition resourceDfn;

    // List of volumes of this resource
    private TransactionMap<VolumeNumber, Volume> volumeMap;

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

    private ResourceDataDatabaseDriver dbDriver;

    /**
     * Constructor used by getInstance
     *
     * @param accCtx
     * @param resDfnRef
     * @param nodeRef
     * @param nodeIdRef
     * @param srlGen
     * @param transMgr
     *
     * @throws SQLException
     * @throws AccessDeniedException
     */
    ResourceData(
        AccessContext accCtx,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        NodeId nodeIdRef,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            ObjectProtection.getInstance(
                accCtx,
                transMgr,
                ObjectProtection.buildPath(
                    nodeRef.getName(),
                    resDfnRef.getName()
                ),
                true
            ),
            resDfnRef,
            nodeRef,
            nodeIdRef,
            srlGen,
            transMgr
        );
    }

    /**
     * Constructor used by database drivers and tests
     *
     * @param objProt
     * @param resDfnRef
     * @param nodeRef
     * @param nodeIdRef
     * @param srlGen
     * @param transMgr
     * @throws SQLException
     */
    ResourceData(
        UUID objIdRef,
        ObjectProtection objProtRef,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        NodeId nodeIdRef,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        ErrorCheck.ctorNotNull(ResourceData.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(ResourceData.class, Node.class, nodeRef);
        resNodeId = nodeIdRef;
        resourceDfn = resDfnRef;
        assgNode = nodeRef;
        objId = objIdRef;

        dbDriver = DrbdManage.getResourceDataDatabaseDriver(resDfnRef.getName());

        volumeMap = new TransactionMap<>(
            new TreeMap<VolumeNumber, Volume>(),
            dbDriver.getVolumeMapDriver()
        );
        resourceProps = SerialPropsContainer.createRootContainer(srlGen);
        objProt = objProtRef;
        flags = new RscFlagsImpl(objProt, dbDriver.getStateFlagPersistence());

        transObjs = Arrays.asList(
            resourceDfn,
            volumeMap,
            assgNode,
            flags,
            objProt,
            resourceProps
        );
    }

    public static ResourceData getInstance(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        Node node,
        NodeId nodeId,
        SerialGenerator srlGen,
        TransactionMgr transMgr,
        boolean createIfNotExists
    )
        throws SQLException, AccessDeniedException
    {
        ResourceData resData = null;

        ResourceDataDatabaseDriver driver = DrbdManage.getResourceDataDatabaseDriver(resDfn.getName());

        if (transMgr != null)
        {
            resData = driver.load(transMgr.dbCon, node, srlGen, transMgr);
        }

        if (resData != null)
        {
            resData.objProt.requireAccess(accCtx, AccessType.VIEW);
        }
        else
        {
            if (createIfNotExists)
            {
                resData = new ResourceData(accCtx, resDfn, node, nodeId, srlGen, transMgr);
                // we do not persist resData here, because node.addResource(...) will also trigger the insert
            }
        }

        if (resData != null)
        {
            synchronized (node)
            {
                synchronized (resDfn)
                {
                    node.addResource(accCtx, resData);
                    try
                    {
                        resDfn.addResource(accCtx, resData);
                    }
                    catch (AccessDeniedException accExc)
                    {
                        // Rollback adding the resource to the node
                        node.removeResource(accCtx, resData);
                    }
                }
            }

            resData.initialized();
        }
        return resData;
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
        return volumeMap.get(volNr);
    }

    @Override
    public Volume setVolume(AccessContext accCtx, Volume vol)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        return volumeMap.put(vol.getVolumeDfn().getVolumeNumber(accCtx), vol);
    }

    @Override
    public Iterator<Volume> iterateVolumes()
    {
        return Collections.unmodifiableCollection(volumeMap.values()).iterator();
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

    private static final class RscFlagsImpl extends StateFlagsBits<RscFlags>
    {
        RscFlagsImpl(ObjectProtection objProtRef, StateFlagsPersistence persistenceRef)
        {
            super(objProtRef, StateFlagsBits.getMask(RscFlags.values()), persistenceRef);
        }
    }
}
