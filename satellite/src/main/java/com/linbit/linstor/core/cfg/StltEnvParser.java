package com.linbit.linstor.core.cfg;

import static com.linbit.linstor.core.cfg.LinstorEnvParser.getEnv;

public class StltEnvParser
{
    public static final String LS_KEEP_RES = "LS_KEEP_RES";
    public static final String LS_PLAIN_PORT_OVERRIDE = "LS_PLAIN_PORT_OVERRIDE";
    public static final String LS_OVERRIDE_NODE_NAME = "LS_OVERRIDE_NODE_NAME";
    public static final String LS_BIND_ADDRESS = "LS_BIND_ADDRESS";

    public static void applyTo(StltConfig cfg)
    {
        LinstorEnvParser.applyTo(cfg);

        cfg.setDrbdKeepResPattern(getEnv(LS_KEEP_RES));
        cfg.setNetPort(getEnv(LS_PLAIN_PORT_OVERRIDE, Integer::parseInt));
        cfg.setNetBindAddress(getEnv(LS_BIND_ADDRESS));
        cfg.setStltOverrideNodeName(getEnv(LS_OVERRIDE_NODE_NAME));
    }
}
