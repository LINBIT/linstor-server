package com.linbit.linstor.core.cfg;

import java.util.Set;

public class StltTomlConfig
{
    public static class NETCOM
    {
        private String type;
        private String bind_address;
        private Integer port;

        private String server_certificate;
        private String key_password;
        private String keystore_password;
        private String truststore_password;
        private String trusted_certificates;
        private String ssl_protocol;

        public void applyTo(StltConfig cfg)
        {
            cfg.setStltType(type);

            cfg.setNetBindAddress(bind_address);
            cfg.setNetPort(port);

            cfg.setNetSecureServerCertificate(server_certificate);
            cfg.setNetSecureKeyPassword(key_password);
            cfg.setNetSecureKeystorePassword(keystore_password);
            cfg.setNetSecureTrustedCertificates(trusted_certificates);
            cfg.setNetSecureTruststorePassword(truststore_password);
            cfg.setNetSecureSslProtocol(ssl_protocol);
        }
    }

    public static class Logging
    {
        private String level;
        private String linstor_level;

        public void applyTo(StltConfig cfg)
        {
            cfg.setLogLevel(level);
            cfg.setLogLevelLinstor(linstor_level);
        }
    }

    static class Files
    {
        private Set<String> allowExtFiles;

        public void applyTo(StltConfig cfg)
        {
            cfg.setExternalFilesWhitelist(allowExtFiles);
        }
    }

    private NETCOM netcom = new NETCOM();
    private Logging logging = new Logging();
    private Files files = new Files();

    public void applyTo(StltConfig cfg)
    {
        netcom.applyTo(cfg);
        logging.applyTo(cfg);
        files.applyTo(cfg);
    }
}
