package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.UUID;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ConnectionDefinition
{
    public UUID getUuid();

    public ResourceDefinition getResourceDefinition(AccessContext accCtx) throws AccessDeniedException;

    public Node getSourceNode(AccessContext accCtx) throws AccessDeniedException;

    public Node getTargetNode(AccessContext accCtx) throws AccessDeniedException;

    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;

    public int getConnectionNumber(AccessContext accCtx) throws AccessDeniedException;

    public void setConnectionNr(AccessContext accCtx, int conNr) throws AccessDeniedException;
}
