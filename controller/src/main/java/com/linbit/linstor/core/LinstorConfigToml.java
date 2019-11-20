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
        private String truststore;
        private String truststore_password = "";

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

        public String getTruststore()
        {
            return truststore;
        }

        public String getTruststorePassword()
        {
            return truststore_password;
        }
    }

    public static class LDAP
    {
        private boolean enabled = false;
        private boolean allow_public_access = false;
        private String uri = "ldap://localhost";
        private String dn = "uid={user}";
        private String search_base = "";
        private String search_filter = "";

        public boolean isEnabled()
        {
            return enabled;
        }
        public boolean allowPublicAccess()
        {
            return allow_public_access;
        }

        public String getUri()
        {
            return uri;
        }

        public String getDN()
        {
            return dn;
        }

        public String getSearchBase()
        {
            return search_base;
        }

        public String getSearchFilter()
        {
            return search_filter;
        }
    }

    public static class DB
    {
        private String user;
        private String password;
        private String connection_url = "jdbc:h2:/var/lib/linstor/linstordb";
        private String ca_certificate;
        private String client_certificate;

        /**
         * Typo in linstor version 1.2.1
         */
        @Deprecated
        private String client_key_pcks8_pem;
        private String client_key_pkcs8_pem;
        private String client_key_password;

        public String getUser()
        {
            return user;
        }

        public String getPassword()
        {
            return password;
        }

        public String getConnectionUrl()
        {
            return connection_url;
        }

        public String getCACertificate()
        {
            return ca_certificate;
        }

        public String getClientCertificate()
        {
            return client_certificate;
        }

        public String getClientKeyPKCS8PEM()
        {
            return client_key_pkcs8_pem != null ? client_key_pkcs8_pem : client_key_pcks8_pem;
        }

        public String getClientKeyPassword()
        {
            return client_key_password;
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

    private HTTP http = new HTTP();
    private HTTPS https = new HTTPS();
    private LDAP ldap = new LDAP();
    private DB db = new DB();
    private Logging logging = new Logging();

    public HTTP getHTTP()
    {
        return http;
    }

    public HTTPS getHTTPS()
    {
        return https;
    }

    public LDAP getLDAP()
    {
        return ldap;
    }

    public DB getDB()
    {
        return db;
    }

    public Logging getLogging()
    {
        return logging;
    }
}
