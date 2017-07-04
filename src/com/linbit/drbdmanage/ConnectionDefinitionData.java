package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.UUID;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;

/**
 * Defines a connection for a DRBD resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ConnectionDefinitionData implements ConnectionDefinition
{
    // Object identifier
    private UUID objId;

    private ObjectProtection objProt;

    private ResourceDefinition resDfn;

    private Node sourceNode;

    private Node targetNode;

    ConnectionDefinitionData(AccessContext accCtx, ResourceDefinition resDfn, Node node1, Node node2, TransactionMgr transMgr) throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            ObjectProtection.getInstance(
                accCtx,
                transMgr,
                ObjectProtection.buildPath(node1.getName(), node2.getName()),
                true
            ),
            resDfn,
            node1,
            node2
        );
    }

    public ConnectionDefinitionData(UUID uuid, ObjectProtection objProtRef, ResourceDefinition resDfnRef, Node node1, Node node2)
    {
        objId = uuid;
        objProt = objProtRef;
        resDfn = resDfnRef;

        if (node1.getName().compareTo(node2.getName()) < 0)
        {
            sourceNode = node1;
            targetNode = node2;
        }
        else
        {
            sourceNode = node2;
            targetNode = node1;
        }
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public ResourceDefinition getResourceDefinition(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resDfn;
    }

    @Override
    public Node getSourceNode(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return sourceNode;
    }

    @Override
    public Node getTargetNode(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return targetNode;
    }
}
