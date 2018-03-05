package com.linbit.linstor;

import java.sql.SQLException;
import java.util.UUID;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

public interface SatelliteConnection extends TransactionObject, DbgInstanceUuid
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

    Node getNode();

    NetInterface getNetInterface();

    TcpPortNumber getPort();

    TcpPortNumber setPort(AccessContext accCtx, TcpPortNumber newPort)
        throws AccessDeniedException, SQLException;

    EncryptionType getEncryptionType();

    EncryptionType setEncryptionType(AccessContext accCtx, EncryptionType newEncryptionType)
        throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    interface SatelliteConnectionApi
    {
        String getNetInterfaceName();
        int getPort();
        String getEncryptionType();
    }

}
