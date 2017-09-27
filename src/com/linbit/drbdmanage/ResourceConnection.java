package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.UUID;

import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ResourceConnection
{
    public UUID getUuid();

    public Resource getSourceResource(AccessContext accCtx) throws AccessDeniedException;

    public Resource getTargetResource(AccessContext accCtx) throws AccessDeniedException;

    public Props getProps(AccessContext accCtx) throws AccessDeniedException;

    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;
}
