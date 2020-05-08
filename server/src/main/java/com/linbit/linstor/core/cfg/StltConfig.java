package com.linbit.linstor.core.cfg;

import com.linbit.linstor.InternalApiConsts;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import com.moandjiezana.toml.Toml;

public class StltConfig extends LinstorConfig
{
    private String stltOverrideNodeName;
    private boolean openflex;

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

    public StltConfig(String[] argsRef)
    {
        super(argsRef);
    }

    public StltConfig()
    {
        super();
    }

    @SuppressWarnings("boxing")
    @Override
    protected void applyDefaultValues()
    {
        super.applyDefaultValues();

        setOpenflex(false);

        setNetBindAddress("::0");
        setNetPort(3366);
        setNetType("plain");

        setNetSecureSslProtocol("TLSv1.2");
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

    public boolean isOpenflex()
    {
        return openflex;
    }

    public void setOpenflex(boolean openflexRef)
    {
        openflex = openflexRef;
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
}
