package com.linbit.linstor;

import java.sql.SQLException;
import java.util.UUID;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public interface NodeConnection extends DbgInstanceUuid
{
    UUID getUuid();

    Node getSourceNode(AccessContext accCtx) throws AccessDeniedException;

    Node getTargetNode(AccessContext accCtx) throws AccessDeniedException;

    Props getProps(AccessContext accCtx) throws AccessDeniedException;

    void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;
}
