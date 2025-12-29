package com.linbit.linstor.core.cfg;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;

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
    private @Nullable String dbUser;
    private @Nullable String dbPassword;
    private @Nullable String dbConnectionUrl;
    private @Nullable String dbCaCertificate;
    private @Nullable String dbClientCertificate;
    private @Nullable String dbClientKeyPkcs8Pem;
    private @Nullable String dbClientKeyPassword;

    private @Nullable String dbInMemory;
    private boolean dbDisableVersionCheck;

    /*
     * Database.k8s
     */
    private int k8sRequestRetries = 3;

    private int k8sMaxRollbackEntries = 100;

    /*
     * Logging
     */
    private @Nullable String logRestAccessLogPath;
    private @Nullable RestAccessLogMode logRestAccessMode;

    /*
     * REST
     */
    private boolean restEnabled;
    private @Nullable String restBindAddress;
    private @Nullable Integer restBindPort;

    /*
     * REST.secure
     */
    private boolean restSecureEnabled;
    private @Nullable String restSecureBindAddress;
    private @Nullable Integer restSecureBindPort;
    private @Nullable String restSecureKeystore;
    private @Nullable String restSecureKeystorePassword;
    private @Nullable String restSecureTruststore;
    private @Nullable String restSecureTruststorePassword;

    /*
     * LDAP
     */
    private boolean ldapEnabled;
    private boolean ldapAllowPublicAccess;
    private @Nullable String ldapUri;
    private @Nullable String ldapDn;
    private @Nullable String ldapSearchBase;
    private @Nullable String ldapSearchFilter;

    /*
     * Encryption
     */
    private @Nullable String masterPassphrase;

    /*
     * Web-UI
     */
    private @Nullable String webUiDirectory;

    public CtrlConfig(@Nullable String[] args)
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

    public void setDbUser(@Nullable String dbUserRef)
    {
        if (dbUserRef != null)
        {
            dbUser = dbUserRef;
        }
    }

    public void setDbPassword(@Nullable String dbPasswordRef)
    {
        if (dbPasswordRef != null)
        {
            dbPassword = dbPasswordRef;
        }
    }

    public void setDbConnectionUrl(@Nullable String dbConnectionUrlRef)
    {
        if (dbConnectionUrlRef != null)
        {
            dbConnectionUrl = dbConnectionUrlRef;
        }
    }

    public void setDbCaCertificate(@Nullable String dbCaCertificateRef)
    {
        if (dbCaCertificateRef != null)
        {
            dbCaCertificate = dbCaCertificateRef;
        }
    }

    public void setDbClientCertificate(@Nullable String dbClientCertificateRef)
    {
        if (dbClientCertificateRef != null)
        {
            dbClientCertificate = dbClientCertificateRef;
        }
    }

    public void setDbClientKeyPkcs8Pem(@Nullable String dbClientKeyPkcs8PemRef)
    {
        if (dbClientKeyPkcs8PemRef != null)
        {
            dbClientKeyPkcs8Pem = dbClientKeyPkcs8PemRef;
        }
    }

    public void setDbClientKeyPassword(@Nullable String dbClientKeyPasswordRef)
    {
        if (dbClientKeyPasswordRef != null)
        {
            dbClientKeyPassword = dbClientKeyPasswordRef;
        }
    }

    public void setDbInMemory(@Nullable String dbInMemoryRef)
    {
        if (dbInMemoryRef != null)
        {
            dbInMemory = dbInMemoryRef;
        }
    }

    public void setDbDisableVersionCheck(@Nullable Boolean dbDisableVersionCheckRef)
    {
        if (dbDisableVersionCheckRef != null)
        {
            dbDisableVersionCheck = dbDisableVersionCheckRef;
        }
    }

    public void setK8sRequestRetries(final @Nullable Integer k8sRequestRetriesRef)
    {
        if (k8sRequestRetriesRef != null)
        {
            k8sRequestRetries = k8sRequestRetriesRef;
        }
    }

    public void setK8sMaxRollbackEntries(final @Nullable Integer k8sMaxRollbackEntriesRef)
    {
        if (k8sMaxRollbackEntriesRef != null)
        {
            k8sMaxRollbackEntries = k8sMaxRollbackEntriesRef;
        }
    }

    public void setLogRestAccessLogPath(@Nullable String logRestAccessLogPathRef)
    {
        if (logRestAccessLogPathRef != null)
        {
            logRestAccessLogPath = logRestAccessLogPathRef;
        }
    }

    public void setLogRestAccessMode(@Nullable RestAccessLogMode logRestAccessModeRef)
    {
        if (logRestAccessModeRef != null)
        {
            logRestAccessMode = logRestAccessModeRef;
        }
    }

    public void setRestEnabled(@Nullable Boolean restEnabledRef)
    {
        if (restEnabledRef != null)
        {
            restEnabled = restEnabledRef;
        }
    }

    public void setRestBindAddress(@Nullable String restBindAddressRef)
    {
        if (restBindAddressRef != null)
        {
            restBindAddress = restBindAddressRef;
        }
    }

    public void setRestBindPort(@Nullable Integer restBindPortRef)
    {
        if (restBindPortRef != null)
        {
            restBindPort = restBindPortRef;
        }
    }

    public void setRestSecureEnabled(@Nullable Boolean restSecureEnabledRef)
    {
        if (restSecureEnabledRef != null)
        {
            restSecureEnabled = restSecureEnabledRef;
        }
    }

    public void setRestSecureBindAddress(@Nullable String restSecureBindAddressRef)
    {
        if (restSecureBindAddressRef != null)
        {
            restSecureBindAddress = restSecureBindAddressRef;
        }
    }

    public void setRestSecureBindPort(@Nullable Integer restSecureBindPortRef)
    {
        if (restSecureBindPortRef != null)
        {
            restSecureBindPort = restSecureBindPortRef;
        }
    }

    public void setRestSecureKeystore(@Nullable String restSecureKeystoreRef)
    {
        if (restSecureKeystoreRef != null)
        {
            restSecureKeystore = restSecureKeystoreRef;
        }
    }

    public void setRestSecureKeystorePassword(@Nullable String restSecureKeystorePasswordRef)
    {
        if (restSecureKeystorePasswordRef != null)
        {
            restSecureKeystorePassword = restSecureKeystorePasswordRef;
        }
    }

    public void setRestSecureTruststore(@Nullable String restSecureTruststoreRef)
    {
        if (restSecureTruststoreRef != null)
        {
            restSecureTruststore = restSecureTruststoreRef;
        }
    }

    public void setRestSecureTruststorePassword(@Nullable String restSecureTruststorePasswordRef)
    {
        if (restSecureTruststorePasswordRef != null)
        {
            restSecureTruststorePassword = restSecureTruststorePasswordRef;
        }
    }

    public void setLdapEnabled(@Nullable Boolean ldapEnabledRef)
    {
        if (ldapEnabledRef != null)
        {
            ldapEnabled = ldapEnabledRef;
        }
    }

    public void setLdapAllowPublicAccess(@Nullable Boolean ldapAllowPublicAccessRef)
    {
        if (ldapAllowPublicAccessRef != null)
        {
            ldapAllowPublicAccess = ldapAllowPublicAccessRef;
        }
    }

    public void setLdapUri(@Nullable String ldapUriRef)
    {
        if (ldapUriRef != null)
        {
            ldapUri = ldapUriRef;
        }
    }

    public void setLdapDn(@Nullable String ldapDnRef)
    {
        if (ldapDnRef != null)
        {
            ldapDn = ldapDnRef;
        }
    }

    public void setLdapSearchBase(@Nullable String ldapSearchBaseRef)
    {
        if (ldapSearchBaseRef != null)
        {
            ldapSearchBase = ldapSearchBaseRef;
        }
    }

    public void setLdapSearchFilter(@Nullable String ldapSearchFilterRef)
    {
        if (ldapSearchFilterRef != null)
        {
            ldapSearchFilter = ldapSearchFilterRef;
        }
    }

    public void setMasterPassphrase(@Nullable String passphraseRef)
    {
        if (passphraseRef != null)
        {
            masterPassphrase = passphraseRef;
        }
    }

    public void setWebUiDirectory(@Nullable String webUiDirectoryRef)
    {
        if (webUiDirectoryRef != null)
        {
            webUiDirectory = webUiDirectoryRef;
        }
    }

    public @Nullable String getDbUser()
    {
        return dbUser;
    }

    public @Nullable String getDbPassword()
    {
        return dbPassword;
    }

    public @Nullable String getDbConnectionUrl()
    {
        return dbConnectionUrl;
    }

    public @Nullable String getDbCaCertificate()
    {
        return dbCaCertificate;
    }

    public @Nullable String getDbClientCertificate()
    {
        return dbClientCertificate;
    }

    public @Nullable String getDbClientKeyPkcs8Pem()
    {
        return dbClientKeyPkcs8Pem;
    }

    public @Nullable String getDbClientKeyPassword()
    {
        return dbClientKeyPassword;
    }

    public @Nullable String getDbInMemory()
    {
        return dbInMemory;
    }

    public boolean isDbVersionCheckDisabled()
    {
        return dbDisableVersionCheck;
    }

    public int getK8sRequestRetries()
    {
        return k8sRequestRetries;
    }

    public int getK8sMaxRollbackEntries()
    {
        return k8sMaxRollbackEntries;
    }

    public @Nullable String getLogRestAccessLogPath()
    {
        return logRestAccessLogPath;
    }

    public @Nullable RestAccessLogMode getLogRestAccessMode()
    {
        return logRestAccessMode;
    }

    public boolean isRestEnabled()
    {
        return restEnabled;
    }

    public String getRestBindAddressWithPort() throws NullPointerException
    {
        if (restBindAddress == null || restBindPort == null)
        {
            throw new NullPointerException(
                "unable to get address: address is " + restBindAddress + ", port is " + restBindPort
            );
        }
        String addr = restBindAddress;
        if (addr.contains(":"))
        {
            addr = "[" + addr + "]"; // IPv6
        }
        return addr + ":" + restBindPort;
    }

    public @Nullable String getRestBindAddress()
    {
        return restBindAddress;
    }

    public @Nullable Integer getRestBindPort()
    {
        return restBindPort;
    }

    public boolean isRestSecureEnabled()
    {
        return restSecureEnabled;
    }

    public String getRestSecureBindAddressWithPort()
    {
        if (restSecureBindAddress == null || restSecureBindPort == null)
        {
            throw new NullPointerException(
                "unable to get secure address: address is " + restSecureBindAddress + ", port is " + restSecureBindPort
            );
        }
        String addr = restSecureBindAddress;
        if (addr.contains(":"))
        {
            addr = "[" + addr + "]"; // IPv6
        }
        return addr + ":" + restSecureBindPort;
    }

    public @Nullable String getRestSecureBindAddress()
    {
        return restSecureBindAddress;
    }

    public @Nullable Integer getRestSecureBindPort()
    {
        return restSecureBindPort;
    }

    public @Nullable String getRestSecureKeystore()
    {
        return restSecureKeystore;
    }

    public @Nullable String getRestSecureKeystorePassword()
    {
        return restSecureKeystorePassword;
    }

    public @Nullable String getRestSecureTruststore()
    {
        return restSecureTruststore;
    }

    public @Nullable String getRestSecureTruststorePassword()
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

    public @Nullable String getLdapUri()
    {
        return ldapUri;
    }

    public @Nullable String getLdapDn()
    {
        return ldapDn;
    }

    public @Nullable String getLdapSearchBase()
    {
        return ldapSearchBase;
    }

    public @Nullable String getLdapSearchFilter()
    {
        return ldapSearchFilter;
    }

    public @Nullable String getMasterPassphrase()
    {
        return masterPassphrase;
    }

    public @Nullable String getWebUiDirectory()
    {
        return webUiDirectory;
    }
}
