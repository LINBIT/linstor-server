package com.linbit.linstor.core.cfg;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.moandjiezana.toml.Toml;

public class StltConfig extends LinstorConfig
{
    private @Nullable String stltOverrideNodeName;
    private boolean remoteSpdk;
    private boolean ebs;

    private @Nullable String netBindAddress;
    private @Nullable Integer netPort;

    private @Nullable String netType;

    private @Nullable String netSecureServerCertificate;
    private @Nullable String netSecureTrustedCertificates;
    private @Nullable String netSecureKeyPassword;
    private @Nullable String netSecureKeystorePassword;
    private @Nullable String netSecureTruststorePassword;
    private @Nullable String netSecureSslProtocol;

    /*
     * External files
     */
    private @Nullable Set<Path> whitelistedExternalFilePaths;

    public StltConfig(String[] argsRef)
    {
        super(argsRef);
    }

    public StltConfig()
    {
    }

    @SuppressWarnings("boxing")
    @Override
    protected void applyDefaultValues()
    {
        super.applyDefaultValues();

        setNetBindAddress("::0");
        setNetPort(3366);
        setNetType("plain");

        setNetSecureSslProtocol("TLSv1.2");

        setExternalFilesWhitelist(Collections.emptySet()); // just to prevent NPE when checking the set with .contains
    }

    @Override
    protected void applyEnvVars()
    {
        StltEnvParser.applyTo(this);
    }

    @Override
    protected void applyCmdLineArgs(String[] cmdLineArgsRef)
    {
        StltCmdLineArgsParser.parseCommandLine(cmdLineArgsRef, this);
    }

    @Override
    protected void applyTomlArgs()
    {
        Path linstorConfigPath = Paths.get(configDir, LINSTOR_STLT_CONFIG).normalize();
        if (Files.exists(linstorConfigPath))
        {
            System.out.println("Loading configuration file \"" + linstorConfigPath.toString() + "\"");
            try
            {
                StltTomlConfig linstorToml = new Toml().read(linstorConfigPath.toFile()).to(StltTomlConfig.class);
                linstorToml.applyTo(this);
            }
            catch (RuntimeException tomlExc)
            {
                System.err.printf("Error parsing '%s': %s%n", linstorConfigPath.toString(), tomlExc.getMessage());
                System.exit(InternalApiConsts.EXIT_CODE_CONFIG_PARSE_ERROR);
            }
        }
    }

    public boolean isRemoteSpdk()
    {
        return remoteSpdk;
    }

    public void setRemoteSpdk(boolean remoteSpdkRef)
    {
        remoteSpdk = remoteSpdkRef;
    }

    public boolean isEbs()
    {
        return ebs;
    }

    public void setEbs(boolean ebsRef)
    {
        ebs = ebsRef;
    }

    public @Nullable String getNetBindAddress()
    {
        return netBindAddress;
    }

    public void setNetBindAddress(@Nullable String netBindAddressRef)
    {
        if (netBindAddressRef != null)
        {
            netBindAddress = netBindAddressRef;
        }
    }

    public @Nullable Integer getNetPort()
    {
        return netPort;
    }

    public void setNetPort(@Nullable Integer netPortRef)
    {
        if (netPortRef != null)
        {
            netPort = netPortRef;
        }
    }

    public @Nullable String getStltOverrideNodeName()
    {
        return stltOverrideNodeName;
    }

    public void setStltOverrideNodeName(@Nullable String stltOverrideNodeNameRef)
    {
        if (stltOverrideNodeNameRef != null)
        {
            stltOverrideNodeName = stltOverrideNodeNameRef;
        }
    }

    public @Nullable String getStltType()
    {
        return netType;
    }

    public void setStltType(@Nullable String stltTypeRef)
    {
        if (stltTypeRef != null)
        {
            netType = stltTypeRef;
        }
    }

    public @Nullable String getNetSecureServerCertificate()
    {
        return netSecureServerCertificate;
    }

    public void setNetSecureServerCertificate(@Nullable String netSecureServerCertificateRef)
    {
        if (netSecureServerCertificateRef != null)
        {
            netSecureServerCertificate = netSecureServerCertificateRef;
        }
    }

    public @Nullable String getNetSecureTrustedCertificates()
    {
        return netSecureTrustedCertificates;
    }

    public void setNetSecureTrustedCertificates(@Nullable String netSecureTrustedCertificatesRef)
    {
        if (netSecureTrustedCertificatesRef != null)
        {
            netSecureTrustedCertificates = netSecureTrustedCertificatesRef;
        }
    }

    public @Nullable String getNetSecureKeyPassword()
    {
        return netSecureKeyPassword;
    }

    public void setNetSecureKeyPassword(@Nullable String netSecureKeyPasswordRef)
    {
        if (netSecureKeyPasswordRef != null)
        {
            netSecureKeyPassword = netSecureKeyPasswordRef;
        }
    }

    public @Nullable String getNetSecureKeystorePassword()
    {
        return netSecureKeystorePassword;
    }

    public void setNetSecureKeystorePassword(@Nullable String netSecureKeystorePasswordRef)
    {
        if (netSecureKeystorePasswordRef != null)
        {
            netSecureKeystorePassword = netSecureKeystorePasswordRef;
        }
    }

    public @Nullable String getNetSecureTruststorePassword()
    {
        return netSecureTruststorePassword;
    }

    public void setNetSecureTruststorePassword(@Nullable String netSecureTruststorePasswordRef)
    {
        if (netSecureTruststorePasswordRef != null)
        {
            netSecureTruststorePassword = netSecureTruststorePasswordRef;
        }
    }

    public @Nullable String getNetSecureSslProtocol()
    {
        return netSecureSslProtocol;
    }

    public void setNetSecureSslProtocol(@Nullable String netSecureSslProtocolRef)
    {
        if (netSecureSslProtocolRef != null)
        {
            netSecureSslProtocol = netSecureSslProtocolRef;
        }
    }

    public @Nullable String getNetType()
    {
        return netType;
    }

    public void setNetType(@Nullable String netTypeRef)
    {
        if (netTypeRef != null)
        {
            netType = netTypeRef;
        }
    }

    public @Nullable Set<Path> getWhitelistedExternalFilePaths()
    {
        return whitelistedExternalFilePaths;
    }

    public void setExternalFilesWhitelist(@Nullable Set<String> whitelistedExternalFilePathsRef)
    {
        if (whitelistedExternalFilePathsRef != null)
        {
            if (whitelistedExternalFilePaths == null)
            {
                whitelistedExternalFilePaths = new HashSet<>();
            }
            else
            {
                whitelistedExternalFilePaths.clear();
            }

            for (String pathStr : whitelistedExternalFilePathsRef)
            {
                whitelistedExternalFilePaths.add(Paths.get(pathStr));
            }
        }
    }
}
