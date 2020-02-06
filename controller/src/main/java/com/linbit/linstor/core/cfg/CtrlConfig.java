package com.linbit.linstor.core.cfg;

import com.linbit.linstor.InternalApiConsts;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.moandjiezana.toml.Toml;

public class CtrlConfig extends LinstorConfig
{
    public static final String DEFAULT_HTTP_LISTEN_ADDRESS = "::";
    public static final int DEFAULT_HTTP_REST_PORT = 3370;
    public static final int DEFAULT_HTTPS_REST_PORT = 3371;

    public static final String DEFAULT_HTTP_BIND_ADDR_WITH_PORT =
        "[" + DEFAULT_HTTP_LISTEN_ADDRESS + "]:" + DEFAULT_HTTP_REST_PORT;
    public static final String DEFAULT_HTTPS_BIND_ADDR_WITH_PORT =
        "[" + DEFAULT_HTTP_LISTEN_ADDRESS + "]:" + DEFAULT_HTTPS_REST_PORT;

    /*
     * Database
     */
    private String dbUser;
    private String dbPassword;
    private String dbConnectionUrl;
    private String dbCaCertificate;
    private String dbClientCertificate;
    private String dbClientKeyPkcs8Pem;
    private String dbClientKeyPassword;

    private String dbInMemory;
    private boolean dbDisableVersionCheck;

    /*
     * Database.ETCD
     */
    private int etcdOperationsPerTransaction = 128;

    /*
     * Logging
     */
    private String logRestAccessLogPath;
    private RestAccessLogMode logRestAccessMode;

    /*
     * REST
     */
    private boolean restEnabled;
    private String restBindAddressWithPort;

    /*
     * REST.secure
     */
    private boolean restSecureEnabled;
    private String restSecureBindAddressWithPort;
    private String restSecureKeystore;
    private String restSecureKeystorePassword;
    private String restSecureTruststore;
    private String restSecureTruststorePassword;

    /*
     * LDAP
     */
    private boolean ldapEnabled;
    private boolean ldapAllowPublicAccess;
    private String ldapUri;
    private String ldapDn;
    private String ldapSearchBase;
    private String ldapSearchFilter;

    /*
     * Encryption
     */
    private String masterPassphrase;

    public CtrlConfig(String[] args)
    {
        super(args);
    }

    @SuppressWarnings("boxing")
    @Override
    protected void applyDefaultValues()
    {
        super.applyDefaultValues();
        setDbConnectionUrl("jdbc:h2:/var/lib/linstor/linstordb");
        setDbDisableVersionCheck(false);

        setLogRestAccessLogPath("rest-access.log");
        setLogRestAccessMode(RestAccessLogMode.APPEND);

        setRestEnabled(true);
        setRestBindAddressWithPort(DEFAULT_HTTP_BIND_ADDR_WITH_PORT);

        setRestSecureEnabled(true);
        setRestSecureBindAddressWithPort(DEFAULT_HTTPS_BIND_ADDR_WITH_PORT);
        setRestSecureKeystorePassword("");
        setRestSecureTruststorePassword("");

        setLdapEnabled(false);
        setLdapAllowPublicAccess(false);
        setLdapUri("ldap://localhost");
        setLdapDn("uid={user}");
        setLdapSearchBase("");
        setLdapSearchFilter("");
    }

    @Override
    protected void applyEnvVars()
    {
        CtrlEnvParser.applyTo(this);
    }

    @Override
    protected void applyTomlArgs()
    {
        Path linstorConfigPath = Paths.get(configDir, LINSTOR_CTRL_CONFIG).normalize();
        if (Files.exists(linstorConfigPath))
        {
            try
            {
                CtrlTomlConfig linstorToml = new Toml().read(linstorConfigPath.toFile()).to(CtrlTomlConfig.class);
                linstorToml.applyTo(this);
            }
            catch (RuntimeException tomlExc)
            {
                System.err.printf("Error parsing '%s': %s%n", linstorConfigPath.toString(), tomlExc.getMessage());
                System.exit(InternalApiConsts.EXIT_CODE_CONFIG_PARSE_ERROR);
            }
        }
    }

    @Override
    protected void applyCmdLineArgs(String[] cmdLineArgs)
    {
        CtrlCmdLineArgsParser.parseCommandLine(cmdLineArgs, this);
    }

    public void setDbUser(String dbUserRef)
    {
        if (dbUserRef != null)
        {
            dbUser = dbUserRef;
        }
    }

    public void setDbPassword(String dbPasswordRef)
    {
        if (dbPasswordRef != null)
        {
            dbPassword = dbPasswordRef;
        }
    }

    public void setDbConnectionUrl(String dbConnectionUrlRef)
    {
        if (dbConnectionUrlRef != null)
        {
            dbConnectionUrl = dbConnectionUrlRef;
        }
    }

    public void setDbCaCertificate(String dbCaCertificateRef)
    {
        if (dbCaCertificateRef != null)
        {
            dbCaCertificate = dbCaCertificateRef;
        }
    }

    public void setDbClientCertificate(String dbClientCertificateRef)
    {
        if (dbClientCertificateRef != null)
        {
            dbClientCertificate = dbClientCertificateRef;
        }
    }

    public void setDbClientKeyPkcs8Pem(String dbClientKeyPkcs8PemRef)
    {
        if (dbClientKeyPkcs8PemRef != null)
        {
            dbClientKeyPkcs8Pem = dbClientKeyPkcs8PemRef;
        }
    }

    public void setDbClientKeyPassword(String dbClientKeyPasswordRef)
    {
        if (dbClientKeyPasswordRef != null)
        {
            dbClientKeyPassword = dbClientKeyPasswordRef;
        }
    }

    public void setDbInMemory(String dbInMemoryRef)
    {
        if (dbInMemoryRef != null)
        {
            dbInMemory = dbInMemoryRef;
        }
    }

    public void setDbDisableVersionCheck(Boolean dbDisableVersionCheckRef)
    {
        if (dbDisableVersionCheckRef != null)
        {
            dbDisableVersionCheck = dbDisableVersionCheckRef;
        }
    }

    public void setEtcdOperationsPerTransaction(Integer etcdOperationsPerTransactionRef)
    {
        if (etcdOperationsPerTransactionRef != null)
        {
            etcdOperationsPerTransaction = etcdOperationsPerTransactionRef;
        }
    }

    public void setLogRestAccessLogPath(String logRestAccessLogPathRef)
    {
        if (logRestAccessLogPathRef != null)
        {
            logRestAccessLogPath = logRestAccessLogPathRef;
        }
    }

    public void setLogRestAccessMode(RestAccessLogMode logRestAccessModeRef)
    {
        if (logRestAccessModeRef != null)
        {
            logRestAccessMode = logRestAccessModeRef;
        }
    }

    public void setRestEnabled(Boolean restEnabledRef)
    {
        if (restEnabledRef != null)
        {
            restEnabled = restEnabledRef;
        }
    }

    public void setRestBindAddressWithPort(String restBindAddressWithPortRef)
    {
        if (restBindAddressWithPortRef != null)
        {
            restBindAddressWithPort = restBindAddressWithPortRef;
        }
    }

    public void setRestSecureEnabled(Boolean restSecureEnabledRef)
    {
        if (restSecureEnabledRef != null)
        {
            restSecureEnabled = restSecureEnabledRef;
        }
    }

    public void setRestSecureBindAddressWithPort(String restSecureBindAddressWithPortRef)
    {
        if (restSecureBindAddressWithPortRef != null)
        {
            restSecureBindAddressWithPort = restSecureBindAddressWithPortRef;
        }
    }

    public void setRestSecureKeystore(String restSecureKeystoreRef)
    {
        if (restSecureKeystoreRef != null)
        {
            restSecureKeystore = restSecureKeystoreRef;
        }
    }

    public void setRestSecureKeystorePassword(String restSecureKeystorePasswordRef)
    {
        if (restSecureKeystorePasswordRef != null)
        {
            restSecureKeystorePassword = restSecureKeystorePasswordRef;
        }
    }

    public void setRestSecureTruststore(String restSecureTruststoreRef)
    {
        if (restSecureTruststoreRef != null)
        {
            restSecureTruststore = restSecureTruststoreRef;
        }
    }

    public void setRestSecureTruststorePassword(String restSecureTruststorePasswordRef)
    {
        if (restSecureTruststorePasswordRef != null)
        {
            restSecureTruststorePassword = restSecureTruststorePasswordRef;
        }
    }

    public void setLdapEnabled(Boolean ldapEnabledRef)
    {
        if (ldapEnabledRef != null)
        {
            ldapEnabled = ldapEnabledRef;
        }
    }

    public void setLdapAllowPublicAccess(Boolean ldapAllowPublicAccessRef)
    {
        if (ldapAllowPublicAccessRef != null)
        {
            ldapAllowPublicAccess = ldapAllowPublicAccessRef;
        }
    }

    public void setLdapUri(String ldapUriRef)
    {
        if (ldapUriRef != null)
        {
            ldapUri = ldapUriRef;
        }
    }

    public void setLdapDn(String ldapDnRef)
    {
        if (ldapDnRef != null)
        {
            ldapDn = ldapDnRef;
        }
    }

    public void setLdapSearchBase(String ldapSearchBaseRef)
    {
        if (ldapSearchBaseRef != null)
        {
            ldapSearchBase = ldapSearchBaseRef;
        }
    }

    public void setLdapSearchFilter(String ldapSearchFilterRef)
    {
        if (ldapSearchFilterRef != null)
        {
            ldapSearchFilter = ldapSearchFilterRef;
        }
    }

    public void setMasterPassphrase(String passphraseRef)
    {
        if (passphraseRef != null)
        {
            masterPassphrase = passphraseRef;
        }
    }

    public String getDbUser()
    {
        return dbUser;
    }

    public String getDbPassword()
    {
        return dbPassword;
    }

    public String getDbConnectionUrl()
    {
        return dbConnectionUrl;
    }

    public String getDbCaCertificate()
    {
        return dbCaCertificate;
    }

    public String getDbClientCertificate()
    {
        return dbClientCertificate;
    }

    public String getDbClientKeyPkcs8Pem()
    {
        return dbClientKeyPkcs8Pem;
    }

    public String getDbClientKeyPassword()
    {
        return dbClientKeyPassword;
    }

    public String getDbInMemory()
    {
        return dbInMemory;
    }

    public boolean isDbVersionCheckDisabled()
    {
        return dbDisableVersionCheck;
    }

    public int getEtcdOperationsPerTransaction()
    {
        return etcdOperationsPerTransaction;
    }

    public String getLogRestAccessLogPath()
    {
        return logRestAccessLogPath;
    }

    public RestAccessLogMode getLogRestAccessMode()
    {
        return logRestAccessMode;
    }

    public boolean isRestEnabled()
    {
        return restEnabled;
    }

    public String getRestBindAddressWithPort()
    {
        return restBindAddressWithPort;
    }

    public boolean isRestSecureEnabled()
    {
        return restSecureEnabled;
    }

    public String getRestSecureBindAddressWithPort()
    {
        return restSecureBindAddressWithPort;
    }

    public String getRestSecureKeystore()
    {
        return restSecureKeystore;
    }

    public String getRestSecureKeystorePassword()
    {
        return restSecureKeystorePassword;
    }

    public String getRestSecureTruststore()
    {
        return restSecureTruststore;
    }

    public String getRestSecureTruststorePassword()
    {
        return restSecureTruststorePassword;
    }

    public boolean isLdapEnabled()
    {
        return ldapEnabled;
    }

    public boolean isLdapPublicAccessAllowed()
    {
        return ldapAllowPublicAccess;
    }

    public String getLdapUri()
    {
        return ldapUri;
    }

    public String getLdapDn()
    {
        return ldapDn;
    }

    public String getLdapSearchBase()
    {
        return ldapSearchBase;
    }

    public String getLdapSearchFilter()
    {
        return ldapSearchFilter;
    }

    public String getMasterPassphrase()
    {
        return masterPassphrase;
    }
}
