package com.linbit.linstor.core.cfg;

import com.linbit.linstor.annotation.Nullable;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.linbit.linstor.core.cfg.LinstorEnvParser.getEnv;

public class StltEnvParser
{
    public static final String LS_PLAIN_PORT_OVERRIDE = "LS_PLAIN_PORT_OVERRIDE";
    public static final String LS_OVERRIDE_NODE_NAME = "LS_OVERRIDE_NODE_NAME";
    public static final String LS_BIND_ADDRESS = "LS_BIND_ADDRESS";

    public static final String LS_EXT_FILES = "LS_ALLOW_EXT_FILES";

    public static final String LS_CLIENT_CONF_FILE = "LS_CLIENT_CONF_FILE";

    /** Set by systemd to the RuntimeDirectory= entries of the unit, colon separated. */
    public static final String RUNTIME_DIRECTORY = "RUNTIME_DIRECTORY";

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
        // the runtime directory systemd manages for us beats the compiled-in default,
        // an explicitly configured path still beats both
        cfg.setClientConfFile(clientConfFileIn(getEnv(RUNTIME_DIRECTORY)));
        cfg.setClientConfFile(getEnv(LS_CLIENT_CONF_FILE));

        String extFilesWhitelist = getEnv(LS_EXT_FILES);
        if (extFilesWhitelist != null)
        {
            cfg.setExternalFilesWhitelist(Arrays.stream(extFilesWhitelist.split(",")).collect(Collectors.toSet()));
        }
    }

    static @Nullable String clientConfFileIn(@Nullable String runtimeDirsRef)
    {
        @Nullable String clientConfFile = null;
        if (runtimeDirsRef != null)
        {
            int sepIdx = runtimeDirsRef.indexOf(':');
            String firstDir = sepIdx < 0 ? runtimeDirsRef : runtimeDirsRef.substring(0, sepIdx);
            if (!firstDir.isBlank())
            {
                clientConfFile = Paths.get(firstDir, StltConfig.CLIENT_CONF_FILE_NAME).toString();
            }
        }
        return clientConfFile;
    }
}
