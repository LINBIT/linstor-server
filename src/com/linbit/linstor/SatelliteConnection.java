package com.linbit.linstor;

import java.sql.SQLException;
import java.util.UUID;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public interface SatelliteConnection
{
    public enum EncryptionType
    {
        SSL,
        PLAIN;

        public static EncryptionType valueOfIgnoreCase(String string)
            throws IllegalArgumentException
        {
            return valueOf(string.toUpperCase());
        }
    }

    public UUID getUuid();

    public Node getNode();

    public NetInterface getNetInterface();

    public TcpPortNumber getPort();

    public TcpPortNumber setPort(AccessContext accCtx, TcpPortNumber newPort) throws AccessDeniedException, SQLException;

    public EncryptionType getEncryptionType();

    public EncryptionType setEncryptionType(AccessContext accCtx, EncryptionType newEncryptionType)
        throws AccessDeniedException, SQLException;

    public interface SatelliteConnectionApi
    {
        public String getNetInterfaceName();
        public int getPort();
        public String getEncryptionType();
    }
}
