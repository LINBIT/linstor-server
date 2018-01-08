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
    public UUID getUuid();

    public NetInterfaceName getName();

    public Node getNode();

    public LsIpAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException;

    public void setAddress(AccessContext accCtx, LsIpAddress newAddress)
        throws AccessDeniedException, SQLException;

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public NetInterfaceApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    public interface NetInterfaceApi
    {
        UUID getUuid();
        String getName();
        String getAddress();
    }
}
