package com.linbit.linstor.core;

public class LinstorConfigToml
{
    public static class HTTP
    {
        private boolean enabled = true;
        private String listen_addr = Controller.DEFAULT_HTTP_LISTEN_ADDRESS;
        private int port = Controller.DEFAULT_HTTP_REST_PORT;

        public boolean isEnabled()
        {
            return enabled;
        }

        public String getListenAddr()
        {
            return listen_addr;
        }

        public int getPort()
        {
            return port;
        }
    }

    public static class HTTPS
    {
        private boolean enabled = true;
        private String listen_addr = Controller.DEFAULT_HTTP_LISTEN_ADDRESS;
        private int port = Controller.DEFAULT_HTTPS_REST_PORT;
        private String keystore;
        private String keystore_password = "";

        public boolean isEnabled()
        {
            return enabled;
        }

        public String getListenAddr()
        {
            return listen_addr;
        }

        public int getPort()
        {
            return port;
        }

        public String getKeystore()
        {
            return keystore;
        }

        public String getKeystorePassword()
        {
            return keystore_password;
        }
    }

    private HTTP http = new HTTP();
    private HTTPS https = new HTTPS();

    public HTTP getHTTP()
    {
        return http;
    }

    public HTTPS getHTTPS()
    {
        return https;
    }
}
