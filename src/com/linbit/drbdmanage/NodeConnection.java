package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.UUID;

import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

public interface NodeConnection
{
    public UUID getUuid();

    public Node getSourceNode(AccessContext accCtx) throws AccessDeniedException;

    public Node getTargetNode(AccessContext accCtx) throws AccessDeniedException;

    public Props getProps(AccessContext accCtx) throws AccessDeniedException;

    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;
}
