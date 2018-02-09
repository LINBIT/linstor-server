package com.linbit.linstor;

import com.linbit.TransactionObject;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.sql.SQLException;
import java.util.UUID;

/**
 * Network interface of a node
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface NetInterface extends TransactionObject, DbgInstanceUuid
{
    UUID getUuid();

    NetInterfaceName getName();

    Node getNode();

    LsIpAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException;

    LsIpAddress setAddress(AccessContext accCtx, LsIpAddress newAddress)
        throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    NetInterfaceApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    public interface NetInterfaceApi
    {
        UUID getUuid();
        String getName();
        String getAddress();
    }
}
