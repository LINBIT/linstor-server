package com.linbit.linstor.core;

import static com.linbit.linstor.core.SatelliteNetComInitializer.NET_COM_DEFAULT_ADDR;
import static com.linbit.linstor.core.SatelliteNetComInitializer.NET_COM_DEFAULT_PORT;
import static com.linbit.linstor.core.SatelliteNetComInitializer.NET_COM_DEFAULT_TYPE;

public class SatelliteConfigToml
{
    public static class NETCOM
    {
        private String type = NET_COM_DEFAULT_TYPE;
        private String bind_address = NET_COM_DEFAULT_ADDR;
        private Integer port = NET_COM_DEFAULT_PORT;

        private String server_certificate;
        private String trusted_certificates;
        private String key_password;
        private String keystore_password;
        private String truststore_password;
        private String ssl_protocol = "TLSv1.2";

        public String getType()
        {
            return type;
        }

        public String getBindAddress()
        {
            return bind_address;
        }

        public Integer getPort()
        {
            return port;
        }

        public String getServerCertificate()
        {
            return server_certificate;
        }

        public String getTrustedCertificates()
        {
            return trusted_certificates;
        }

        public String getKeyPassword()
        {
            return key_password;
        }

        public String getKeystorePassword()
        {
            return keystore_password;
        }

        public String getTrustStorePassword()
        {
            return truststore_password;
        }

        public String getSslProtocol()
        {
            return ssl_protocol;
        }
    }

    public static class Logging
    {
        private String level = "info";

        public String getLevel()
        {
            return level;
        }
    }

    private NETCOM netcom = new NETCOM();
    private Logging logging = new Logging();

    public NETCOM getNETCOM()
    {
        return netcom;
    }
    public Logging getLogging()
    {
        return logging;
    }
}
