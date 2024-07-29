package com.linbit.linstor.core.cfg;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.LinstorConfigTool;
import com.linbit.linstor.core.cfg.LinstorConfig.RestAccessLogMode;

@SuppressWarnings("checkstyle:MemberName")
public class CtrlTomlConfig
{
    static class HTTP
    {
        private @Nullable Boolean enabled;
        private @Nullable String listen_addr;
        private @Nullable Integer port;

        public void applyTo(CtrlConfig cfg)
        {
            cfg.setRestEnabled(enabled);
            cfg.setRestBindAddress(listen_addr);
            cfg.setRestBindPort(port);
        }
    }

    static class HTTPS
    {
        private @Nullable Boolean enabled;
        private @Nullable String listen_addr;
        private @Nullable Integer port;
        private @Nullable String keystore;
        private @Nullable String keystore_password;
        private @Nullable String truststore;
        private @Nullable String truststore_password;

        public void applyTo(CtrlConfig cfg)
        {
            cfg.setRestSecureEnabled(enabled);
            cfg.setRestSecureBindAddress(listen_addr);
            cfg.setRestSecureBindPort(port);
            cfg.setRestSecureKeystore(keystore);
            cfg.setRestSecureKeystorePassword(keystore_password);
            cfg.setRestSecureTruststore(truststore);
            cfg.setRestSecureTruststorePassword(truststore_password);
        }
    }

    static class LDAP
    {
        private @Nullable Boolean enabled;
        private @Nullable Boolean allow_public_access;
        private @Nullable String uri;
        private @Nullable String dn;
        private @Nullable String search_base;
        private @Nullable String search_filter;

        public void applyTo(CtrlConfig cfg)
        {
            cfg.setLdapEnabled(enabled);
            cfg.setLdapAllowPublicAccess(allow_public_access);
            cfg.setLdapUri(uri);
            cfg.setLdapDn(dn);
            cfg.setLdapSearchBase(search_base);
            cfg.setLdapSearchFilter(search_filter);
        }
    }

    public static class DB
    {
        private @Nullable String user;
        private @Nullable String password;
        private @Nullable String connection_url;
        private @Nullable String ca_certificate;
        private @Nullable String client_certificate;
        /**
         * Typo in linstor version 1.2.1
         */
        @Deprecated
        private @Nullable String client_key_pcks8_pem;
        private @Nullable String client_key_pkcs8_pem;
        private @Nullable String client_key_password;

        private @Nullable Etcd etcd = new Etcd();

        private @Nullable K8s k8s = new K8s();

        public void applyTo(CtrlConfig cfg)
        {
            cfg.setDbUser(user);
            cfg.setDbPassword(password);
            cfg.setDbConnectionUrl(connection_url);
            cfg.setDbCaCertificate(ca_certificate);
            cfg.setDbClientCertificate(client_certificate);
            cfg.setDbClientKeyPkcs8Pem(client_key_pkcs8_pem != null ? client_key_pkcs8_pem : client_key_pcks8_pem);
            cfg.setDbClientKeyPassword(client_key_password);

            etcd.applyTo(cfg);
            k8s.applyTo(cfg);
        }

        /**
         * Getter needed by {@link LinstorConfigTool}
         */
        public @Nullable String getConnectionUrl()
        {
            return connection_url;
        }

        /**
         * Getter needed by {@link LinstorConfigTool}
         */
        public @Nullable String getUser()
        {
            return user;
        }

        /**
         * Getter needed by {@link LinstorConfigTool}
         */
        public @Nullable String getPassword()
        {
            return password;
        }
    }

    static class Logging
    {
        private @Nullable String level;
        private @Nullable String linstor_level;
        private @Nullable String rest_access_log_path;
        private @Nullable RestAccessLogMode rest_access_log_mode;

        public void applyTo(CtrlConfig cfg)
        {
            cfg.setLogLevel(level);
            cfg.setLogLevelLinstor(linstor_level);
            cfg.setLogRestAccessLogPath(rest_access_log_path);
            cfg.setLogRestAccessMode(rest_access_log_mode);
        }
    }

    static class Encrypt
    {
        private @Nullable String passphrase;

        public void applyTo(CtrlConfig cfg)
        {
            cfg.setMasterPassphrase(passphrase);
        }
    }

    static class Etcd
    {
        private @Nullable Integer ops_per_transaction;
        private @Nullable String prefix;

        public void applyTo(CtrlConfig cfg)
        {
            cfg.setEtcdOperationsPerTransaction(ops_per_transaction);
            cfg.setEtcdPrefix(prefix);
        }
    }

    static class K8s
    {
        private @Nullable Integer request_retries;
        private @Nullable Integer max_rollback_entries;

        public void applyTo(CtrlConfig cfg)
        {
            cfg.setK8sRequestRetries(request_retries);
            cfg.setK8sMaxRollbackEntries(max_rollback_entries);
        }
    }

    static class WebUi
    {
        private @Nullable String directory;

        public void applyTo(CtrlConfig cfg)
        {
            cfg.setWebUiDirectory(directory);
        }
    }

    private HTTP http = new HTTP();
    private HTTPS https = new HTTPS();
    private LDAP ldap = new LDAP();
    private DB db = new DB();
    private Logging logging = new Logging();
    private Encrypt encrypt = new Encrypt();
    private WebUi webUi = new WebUi();

    /**
     * Getter needed by {@link LinstorConfigTool}
     */
    public DB getDB()
    {
        return db;
    }

    public void applyTo(CtrlConfig cfg)
    {
        http.applyTo(cfg);
        https.applyTo(cfg);
        ldap.applyTo(cfg);
        db.applyTo(cfg);
        logging.applyTo(cfg);
        encrypt.applyTo(cfg);
        webUi.applyTo(cfg);
    }
}
