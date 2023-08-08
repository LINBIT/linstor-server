package com.linbit.linstor.core.cfg;

import com.linbit.linstor.InternalApiConsts;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.moandjiezana.toml.Toml;

public class CtrlConfig extends LinstorConfig
{
    public static final String DEFAULT_HTTP_LISTEN_ADDRESS = "::";
    public static final String DEFAULT_HTTPS_LISTEN_ADDRESS = "::";
    public static final int DEFAULT_HTTP_REST_PORT = 3370;
    public static final int DEFAULT_HTTPS_REST_PORT = 3371;

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
    private String etcdPrefix;

    /*
     * Database.k8s
     */
    private int k8sRequestRetries = 3;

    private int k8sMaxRollbackEntries = 100;

    /*
     * Logging
     */
    private String logRestAccessLogPath;
    private RestAccessLogMode logRestAccessMode;

    /*
     * REST
     */
    private boolean restEnabled;
    private String restBindAddress;
    private int restBindPort;

    /*
     * REST.secure
     */
    private boolean restSecureEnabled;
    private String restSecureBindAddress;
    private int restSecureBindPort;
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

    /*
     * Web-UI
     */
    private String webUiDirectory;

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
        setEtcdPrefix("/LINSTOR/");

        setLogRestAccessLogPath("rest-access.log");
        setLogRestAccessMode(RestAccessLogMode.NO_LOG);

        setRestEnabled(true);
        setRestBindAddress(DEFAULT_HTTP_LISTEN_ADDRESS);
        setRestBindPort(DEFAULT_HTTP_REST_PORT);

        setRestSecureEnabled(true);
        setRestSecureBindAddress(DEFAULT_HTTPS_LISTEN_ADDRESS);
        setRestSecureBindPort(DEFAULT_HTTPS_REST_PORT);
        setRestSecureKeystorePassword("");
        setRestSecureTruststorePassword("");

        setLdapEnabled(false);
        setLdapAllowPublicAccess(false);
        setLdapUri("ldap://localhost");
        setLdapDn("uid={user}");
        setLdapSearchBase("");
        setLdapSearchFilter("");

        setWebUiDirectory("/usr/share/linstor-server/ui");
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
            System.out.println("Loading configuration file \"" + linstorConfigPath.toString() + "\"");
            try
            {
                CtrlTomlConfig linstorToml = new Toml().read(linstorConfigPath.toFile()).to(CtrlTomlConfig.class);
                linstorToml.applyTo(this);
            }
            catch (RuntimeException tomlExc)
            {
                System.err.printf("Error parsing '%s': %s%n", linstorConfigPath, tomlExc.getMessage());
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

    public void setEtcdPrefix(final String etcdPrefixRef)
    {
        if (etcdPrefixRef != null)
        {
            etcdPrefix = etcdPrefixRef;
        }
    }

    public void setK8sRequestRetries(final Integer k8sRequestRetriesRef)
    {
        if (k8sRequestRetriesRef != null)
        {
            k8sRequestRetries = k8sRequestRetriesRef;
        }
    }

    public void setK8sMaxRollbackEntries(final Integer k8sMaxRollbackEntriesRef)
    {
        if (k8sMaxRollbackEntriesRef != null)
        {
            k8sMaxRollbackEntries = k8sMaxRollbackEntriesRef;
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

    public void setRestBindAddress(String restBindAddressRef)
    {
        if (restBindAddressRef != null)
        {
            restBindAddress = restBindAddressRef;
        }
    }

    public void setRestBindPort(Integer restBindPortRef)
    {
        if (restBindPortRef != null)
        {
            restBindPort = restBindPortRef;
        }
    }

    public void setRestSecureEnabled(Boolean restSecureEnabledRef)
    {
        if (restSecureEnabledRef != null)
        {
            restSecureEnabled = restSecureEnabledRef;
        }
    }

    public void setRestSecureBindAddress(String restSecureBindAddressRef)
    {
        if (restSecureBindAddressRef != null)
        {
            restSecureBindAddress = restSecureBindAddressRef;
        }
    }

    public void setRestSecureBindPort(Integer restSecureBindPortRef)
    {
        if (restSecureBindPortRef != null)
        {
            restSecureBindPort = restSecureBindPortRef;
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

    public void setWebUiDirectory(String webUiDirectoryRef)
    {
        if (webUiDirectoryRef != null)
        {
            webUiDirectory = webUiDirectoryRef;
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

    public String getEtcdPrefix()
    {
        return etcdPrefix;
    }

    public int getK8sRequestRetries()
    {
        return k8sRequestRetries;
    }

    public int getK8sMaxRollbackEntries()
    {
        return k8sMaxRollbackEntries;
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
        String addr = restBindAddress;
        if (addr.contains(":"))
        {
            addr = "[" + addr + "]"; // IPv6
        }
        return addr + ":" + restBindPort;
    }

    public String getRestBindAddress()
    {
        return restBindAddress;
    }

    public int getRestBindPort()
    {
        return restBindPort;
    }

    public boolean isRestSecureEnabled()
    {
        return restSecureEnabled;
    }

    public String getRestSecureBindAddressWithPort()
    {
        String addr = restSecureBindAddress;
        if (addr.contains(":"))
        {
            addr = "[" + addr + "]"; // IPv6
        }
        return addr + ":" + restSecureBindPort;
    }

    public String getRestSecureBindAddress()
    {
        return restSecureBindAddress;
    }

    public int getRestSecureBindPort()
    {
        return restSecureBindPort;
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

    public String getWebUiDirectory()
    {
        return webUiDirectory;
    }
}
