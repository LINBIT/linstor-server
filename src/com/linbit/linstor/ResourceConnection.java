package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ResourceConnection extends DbgInstanceUuid, TransactionObject
{
    UUID getUuid();

    Resource getSourceResource(AccessContext accCtx) throws AccessDeniedException;

    Resource getTargetResource(AccessContext accCtx) throws AccessDeniedException;

    Props getProps(AccessContext accCtx) throws AccessDeniedException;

    void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;

    RscConnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    interface RscConnApi
    {
        UUID getUuid();
        String getSourceNodeName();
        String getTargetNodeName();
        Map<String, String> getProps();
    }
}
