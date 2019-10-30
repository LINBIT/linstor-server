package com.linbit.linstor.core;

import java.util.function.Function;
import java.util.regex.Pattern;

public class SatelliteCmdlArguments extends LinStorCmdlArguments
{
    public static final String LS_KEEP_RES = "LS_KEEP_RES";
    public static final String LS_PLAIN_PORT_OVERRIDE = "LS_PLAIN_PORT_OVERRIDE";
    public static final String LS_OVERRIDE_NODE_NAME = "LS_OVERRIDE_NODE_NAME";
    public static final String LS_BIND_ADDRESS = "LS_BIND_ADDRESS";

    private Pattern keepResPattern;

    private Integer plainPortOverride;
    private String bindAddress;
    private String overrideNodeName;

    public SatelliteCmdlArguments()
    {
        keepResPattern = getEnv(LS_KEEP_RES, Pattern::compile);
        plainPortOverride = getEnv(LS_PLAIN_PORT_OVERRIDE, Integer::parseInt);
        bindAddress = getEnv(LS_BIND_ADDRESS, Function.identity());
        overrideNodeName = getEnv(LS_OVERRIDE_NODE_NAME, Function.identity());
    }

    public Pattern getKeepResPattern()
    {
        return keepResPattern;
    }

    public void setKeepResRegex(String keepResRegex)
    {
        keepResPattern = Pattern.compile(keepResRegex, Pattern.DOTALL);
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
}
