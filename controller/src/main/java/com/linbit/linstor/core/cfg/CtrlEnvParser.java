package com.linbit.linstor.core.cfg;

import com.linbit.utils.Pair;

import static com.linbit.linstor.core.cfg.CtrlCmdLineArgsParser.splitIpPort;
import static com.linbit.linstor.core.cfg.LinstorEnvParser.getEnv;

class CtrlEnvParser
{
    public static final String LS_REST_BIND_ADDRESS = "LS_REST_BIND_ADDRESS";
    public static final String LS_REST_BIND_ADDRESS_SECURE = "LS_REST_BIND_ADDRESS_SECURE";
    public static final String MASTER_PASSPHRASE = "MASTER_PASSPHRASE";

    public static final String WEBUI_DIRECTORY = "LS_WEBUI_DIRECTORY";

    private CtrlEnvParser()
    {
    }

    static void applyTo(CtrlConfig cfg)
    {
        LinstorEnvParser.applyTo(cfg);

        Pair<String, Integer> restBind = splitIpPort(getEnv(LS_REST_BIND_ADDRESS));
        Pair<String, Integer> restSecureBind = splitIpPort(getEnv(LS_REST_BIND_ADDRESS_SECURE));

        cfg.setRestBindAddress(restBind.objA);
        cfg.setRestBindPort(restBind.objB);
        cfg.setRestSecureBindAddress(restSecureBind.objA);
        cfg.setRestSecureBindPort(restSecureBind.objB);

        cfg.setMasterPassphrase(getEnv(MASTER_PASSPHRASE));

        cfg.setWebUiDirectory(getEnv(WEBUI_DIRECTORY));
    }
}
