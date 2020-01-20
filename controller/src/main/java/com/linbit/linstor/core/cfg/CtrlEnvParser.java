package com.linbit.linstor.core.cfg;

import static com.linbit.linstor.core.cfg.LinstorEnvParser.getEnv;

class CtrlEnvParser
{
    public static final String LS_REST_BIND_ADDRESS = "LS_REST_BIND_ADDRESS";
    public static final String LS_REST_BIND_ADDRESS_SECURE = "LS_REST_BIND_ADDRESS_SECURE";

    static void applyTo(CtrlConfig cfg)
    {
        LinstorEnvParser.applyTo(cfg);

        cfg.setRestBindAddressWithPort(getEnv(LS_REST_BIND_ADDRESS));
        cfg.setRestSecureBindAddressWithPort(getEnv(LS_REST_BIND_ADDRESS_SECURE));
    }
}
