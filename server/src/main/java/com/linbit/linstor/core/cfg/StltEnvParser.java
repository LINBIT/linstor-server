package com.linbit.linstor.core.cfg;

import static com.linbit.linstor.core.cfg.LinstorEnvParser.getEnv;

import java.util.Arrays;
import java.util.stream.Collectors;

public class StltEnvParser
{
    public static final String LS_PLAIN_PORT_OVERRIDE = "LS_PLAIN_PORT_OVERRIDE";
    public static final String LS_OVERRIDE_NODE_NAME = "LS_OVERRIDE_NODE_NAME";
    public static final String LS_BIND_ADDRESS = "LS_BIND_ADDRESS";

    public static final String LS_EXT_FILES = "LS_ALLOW_EXT_FILES";

    private StltEnvParser()
    {
    }

    public static void applyTo(StltConfig cfg)
    {
        LinstorEnvParser.applyTo(cfg);

        // Map<String, String> env = System.getenv();
        // for (Entry<String, String> entry : env.entrySet())
        // {
        // System.out.println(entry.getKey() + ": " + entry.getValue());
        // }

        cfg.setNetPort(getEnv(LS_PLAIN_PORT_OVERRIDE, Integer::parseInt));
        cfg.setNetBindAddress(getEnv(LS_BIND_ADDRESS));
        cfg.setStltOverrideNodeName(getEnv(LS_OVERRIDE_NODE_NAME));

        String extFilesWhitelist = getEnv(LS_EXT_FILES);
        if (extFilesWhitelist != null)
        {
            cfg.setExternalFilesWhitelist(Arrays.stream(extFilesWhitelist.split(",")).collect(Collectors.toSet()));
        }
    }
}
