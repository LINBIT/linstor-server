package com.linbit.linstor.core.cfg;

import com.linbit.linstor.annotation.Nullable;

import java.util.function.Function;

public class LinstorEnvParser
{
    public static final String LS_CONFIG_DIRECTORY = "LS_CONFIG_DIRECTORY";
    public static final String LS_LOG_DIRECTORY = "LS_LOG_DIRECTORY";
    public static final String LS_LOG_LEVEL = "LS_LOG_LEVEL";
    public static final String LS_LOG_LEVEL_LINSTOR = "LS_LOG_LEVEL_LINSTOR";

    private LinstorEnvParser()
    {
    }

    public static void applyTo(LinstorConfig cfg)
    {
        cfg.setConfigDir(getEnv(LS_CONFIG_DIRECTORY));
        cfg.setLogDirectory(getEnv(LS_LOG_DIRECTORY));
        cfg.setLogLevel(getEnv(LS_LOG_LEVEL));
        cfg.setLogLevelLinstor(getEnv(LS_LOG_LEVEL_LINSTOR));
    }

    protected static @Nullable String getEnv(String env)
    {
        return getEnv(env, Function.identity(), null);
    }

    protected static <T> @Nullable T getEnv(String env, Function<String, T> func)
    {
        return getEnv(env, func, null);
    }

    protected static <T> @Nullable T getEnv(String env, Function<String, T> func, @Nullable T dfltValue)
    {
        String envVal = System.getenv(env);
        return envVal == null ? dfltValue : func.apply(envVal);
    }
}
