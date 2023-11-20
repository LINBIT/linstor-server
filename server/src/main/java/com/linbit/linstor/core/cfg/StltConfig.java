package com.linbit.linstor.core.cfg;

import com.linbit.linstor.InternalApiConsts;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import com.moandjiezana.toml.Toml;

public class StltConfig extends LinstorConfig
{
    private String stltOverrideNodeName;
    private boolean remoteSpdk;
    private boolean ebs;

    private Pattern drbdKeepResPattern;

    private String netBindAddress;
    private Integer netPort;

    private String netType;

    private String netSecureServerCertificate;
    private String netSecureTrustedCertificates;
    private String netSecureKeyPassword;
    private String netSecureKeystorePassword;
    private String netSecureTruststorePassword;
    private String netSecureSslProtocol;

    /*
     * External files
     */
    private Set<Path> whitelistedExternalFilePaths;

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

    public Pattern getDrbdKeepResPattern()
    {
        return drbdKeepResPattern;
    }

    public void setDrbdKeepResPattern(String drbdKeepResPatternRef)
    {
        if (drbdKeepResPatternRef != null)
        {
            setDrbdKeepResPattern(Pattern.compile(drbdKeepResPatternRef));
        }
    }

    public void setDrbdKeepResPattern(Pattern drbdKeepResPatternRef)
    {
        if (drbdKeepResPatternRef != null)
        {
            drbdKeepResPattern = drbdKeepResPatternRef;
        }
    }

    public String getNetBindAddress()
    {
        return netBindAddress;
    }

    public void setNetBindAddress(String netBindAddressRef)
    {
        if (netBindAddressRef != null)
        {
            netBindAddress = netBindAddressRef;
        }
    }

    public Integer getNetPort()
    {
        return netPort;
    }

    public void setNetPort(Integer netPortRef)
    {
        if (netPortRef != null)
        {
            netPort = netPortRef;
        }
    }

    public String getStltOverrideNodeName()
    {
        return stltOverrideNodeName;
    }

    public void setStltOverrideNodeName(String stltOverrideNodeNameRef)
    {
        if (stltOverrideNodeNameRef != null)
        {
            stltOverrideNodeName = stltOverrideNodeNameRef;
        }
    }

    public String getStltType()
    {
        return netType;
    }

    public void setStltType(String stltTypeRef)
    {
        if (stltTypeRef != null)
        {
            netType = stltTypeRef;
        }
    }

    public String getNetSecureServerCertificate()
    {
        return netSecureServerCertificate;
    }

    public void setNetSecureServerCertificate(String netSecureServerCertificateRef)
    {
        if (netSecureServerCertificateRef != null)
        {
            netSecureServerCertificate = netSecureServerCertificateRef;
        }
    }

    public String getNetSecureTrustedCertificates()
    {
        return netSecureTrustedCertificates;
    }

    public void setNetSecureTrustedCertificates(String netSecureTrustedCertificatesRef)
    {
        if (netSecureTrustedCertificatesRef != null)
        {
            netSecureTrustedCertificates = netSecureTrustedCertificatesRef;
        }
    }

    public String getNetSecureKeyPassword()
    {
        return netSecureKeyPassword;
    }

    public void setNetSecureKeyPassword(String netSecureKeyPasswordRef)
    {
        if (netSecureKeyPasswordRef != null)
        {
            netSecureKeyPassword = netSecureKeyPasswordRef;
        }
    }

    public String getNetSecureKeystorePassword()
    {
        return netSecureKeystorePassword;
    }

    public void setNetSecureKeystorePassword(String netSecureKeystorePasswordRef)
    {
        if (netSecureKeystorePasswordRef != null)
        {
            netSecureKeystorePassword = netSecureKeystorePasswordRef;
        }
    }

    public String getNetSecureTruststorePassword()
    {
        return netSecureTruststorePassword;
    }

    public void setNetSecureTruststorePassword(String netSecureTruststorePasswordRef)
    {
        if (netSecureTruststorePasswordRef != null)
        {
            netSecureTruststorePassword = netSecureTruststorePasswordRef;
        }
    }

    public String getNetSecureSslProtocol()
    {
        return netSecureSslProtocol;
    }

    public void setNetSecureSslProtocol(String netSecureSslProtocolRef)
    {
        if (netSecureSslProtocolRef != null)
        {
            netSecureSslProtocol = netSecureSslProtocolRef;
        }
    }

    public String getNetType()
    {
        return netType;
    }

    public void setNetType(String netTypeRef)
    {
        if (netTypeRef != null)
        {
            netType = netTypeRef;
        }
    }

    public Set<Path> getWhitelistedExternalFilePaths()
    {
        return whitelistedExternalFilePaths;
    }

    public void setExternalFilesWhitelist(Set<String> whitelistedExternalFilePathsRef)
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
