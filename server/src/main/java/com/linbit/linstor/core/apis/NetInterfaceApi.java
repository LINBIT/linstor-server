package com.linbit.linstor.core.apis;

import com.linbit.linstor.annotation.Nullable;

import java.util.UUID;

public interface NetInterfaceApi
{
    UUID getUuid();
    String getName();
    String getAddress();

    @Nullable
    StltConn getStltConn();

    class StltConn
    {
        private final int port;
        private final String encrType;

        public StltConn(int portRef, String encrTypeRef)
        {
            port = portRef;
            encrType = encrTypeRef;
        }

        public int getSatelliteConnectionPort()
        {
            return port;
        }

        public String getSatelliteConnectionEncryptionType()
        {
            return encrType;
        }
    }
}
