package com.linbit.linstor.core;

import java.util.regex.Pattern;

public class SatelliteCmdlArguments extends LinStorCmdlArguments
{
    private Pattern keepResPattern;
    private boolean skipHostnameCheck;

    private Integer plainPortOverride;
    private String bindAddress;

    public SatelliteCmdlArguments()
    {
        keepResPattern = null;
        skipHostnameCheck = false;
        plainPortOverride = null;
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
}
