package com.linbit.linstor.core;

import java.util.regex.Pattern;

public class SatelliteCmdlArguments extends LinStorCmdlArguments
{
    private Pattern keepResPattern;
    private boolean skipHostnameCheck;
    private boolean skipDrbdCheck;

    private Integer plainPortOverride;
    private String bindAddress;
    private String overrideNodeName;

    private String logLevel;

    public SatelliteCmdlArguments()
    {
        keepResPattern = null;
        skipHostnameCheck = false;
        plainPortOverride = null;
        overrideNodeName = null;
        logLevel = null;
    }

    public Pattern getKeepResPattern()
    {
        return keepResPattern;
    }

    public void setKeepResRegex(String keepResRegex)
    {
        keepResPattern = Pattern.compile(keepResRegex, Pattern.DOTALL);
    }

    public void setSkipHostnameCheck(boolean skipHostnameCheckRef)
    {
        skipHostnameCheck = skipHostnameCheckRef;
    }

    public boolean isSkipHostnameCheck()
    {
        return skipHostnameCheck;
    }

    public void setSkipDrbdCheck(boolean checkFlag)
    {
        skipDrbdCheck = checkFlag;
    }

    public boolean isSkipDrbdCheck()
    {
        return skipDrbdCheck;
    }

    public void setOverridePlainPort(Integer plainPortRef)
    {
        plainPortOverride = plainPortRef;
    }

    public Integer getPlainPortOverride()
    {
        return plainPortOverride;
    }

    public void setBindAddress(String bindAddressRef)
    {
        bindAddress = bindAddressRef;
    }

    public String getBindAddress()
    {
        return bindAddress;
    }

    public String getOverrideNodeName()
    {
        return overrideNodeName;
    }

    public void setOverrideNodeName(String overrideNodeNameRef)
    {
        overrideNodeName = overrideNodeNameRef;
    }

    public String getLogLevel()
    {
        return logLevel;
    }

    public void setLogLevel(String logLevelRef)
    {
        logLevel = logLevelRef;
    }
}
