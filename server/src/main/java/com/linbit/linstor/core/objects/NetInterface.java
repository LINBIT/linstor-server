package com.linbit.linstor.core.objects;

import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.UUID;

/**
 * Network interface of a node
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface NetInterface extends TransactionObject, DbgInstanceUuid, Comparable<NetInterface>
{
    enum EncryptionType
    {
        SSL,
        PLAIN;

        public static EncryptionType valueOfIgnoreCase(String string)
            throws IllegalArgumentException
        {
            return valueOf(string.toUpperCase());
        }
    }

    UUID getUuid();

    NetInterfaceName getName();

    Node getNode();

    LsIpAddress getAddress(AccessContext accCtx)
        throws AccessDeniedException;

    LsIpAddress setAddress(AccessContext accCtx, LsIpAddress newAddress)
        throws AccessDeniedException, DatabaseException;

    boolean isUsableAsStltConn(AccessContext accCtx) throws AccessDeniedException;

    boolean setStltConn(AccessContext accCtx, TcpPortNumber port, EncryptionType encrType)
        throws AccessDeniedException, DatabaseException;

    TcpPortNumber getStltConnPort(AccessContext accCtx) throws AccessDeniedException;

    EncryptionType getStltConnEncryptionType(AccessContext accCtx) throws AccessDeniedException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

    NetInterfaceApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    public interface NetInterfaceApi
    {
        UUID getUuid();
        String getName();
        String getAddress();

        boolean isUsableAsSatelliteConnection();

        int getSatelliteConnectionPort();
        String getSatelliteConnectionEncryptionType();
    }



}
