package com.linbit.linstor.core.cfg;

import com.linbit.linstor.annotation.Nullable;

import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class LinstorConfig
{
    public static final String LINSTOR_CTRL_CONFIG = "linstor.toml";
    public static final String LINSTOR_STLT_CONFIG = "linstor_satellite.toml";

    public enum RestAccessLogMode
    {
        APPEND, ROTATE_HOURLY, ROTATE_DAILY, NO_LOG;
    }

    protected @Nullable String configDir;
    protected @Nullable Path configPath;

    /*
     * Debug
     */
    protected boolean debugConsoleEnable = false;

    /*
     * Logging
     */
    protected boolean logPrintStackTrace;
    protected @Nullable String logDirectory;
    protected @Nullable String logLevel;
    protected @Nullable String logLevelLinstor;

    /**
     * Order or priority of config sources (top has highest priority)
     * 1) command line arguments
     * 2) toml file
     * 3) environment variables
     * 4) default values
     *
     * @param cmdLineArgs
     *
     * @return
     */
    public LinstorConfig(@Nullable String[] cmdLineArgs)
    {
        applyDefaultValues();

        // override config (default) with environment values
        applyEnvVars();

        // override config (default + env) with toml config.
        // toml's config file could be set via cmdLineArgs. apply that first
        if (cmdLineArgs != null)
        {
            applyCmdLineArgs(cmdLineArgs);
        }
        applyTomlArgs();

        // override config (default + env + toml) with cmd line args
        if (cmdLineArgs != null)
        {
            applyCmdLineArgs(cmdLineArgs);
        }
    }

    public LinstorConfig()
    {
    }

    protected void applyDefaultValues()
    {
        setConfigDir("./");
        setDebugConsoleEnable(false);
        setLogDirectory("./logs");
        setLogLevel("INFO");
        // logLevelLinstor stays null. if null, it will inherit value from logLevel
    }

    protected abstract void applyEnvVars();

    protected abstract void applyCmdLineArgs(String[] cmdLineArgs);

    protected abstract void applyTomlArgs();

    public void setConfigDir(@Nullable String configDirRef)
    {
        if (configDirRef != null)
        {
            configDir = configDirRef;
            configPath = Paths.get(configDir);
        }
    }

    public void setDebugConsoleEnable(@Nullable Boolean debugConsoleEnableRef)
    {
        if (debugConsoleEnableRef != null)
        {
            debugConsoleEnable = debugConsoleEnableRef;
        }
    }

    public void setLogPrintStackTrace(@Nullable Boolean logPrintStackTraceRef)
    {
        if (logPrintStackTraceRef != null)
        {
            logPrintStackTrace = logPrintStackTraceRef;
        }
    }

    public void setLogDirectory(@Nullable String logDirectoryRef)
    {
        if (logDirectoryRef != null)
        {
            logDirectory = logDirectoryRef;
        }
    }

    public void setLogLevel(@Nullable String logLevelRef)
    {
        if (logLevelRef != null)
        {
            logLevel = logLevelRef;
        }
    }

    public void setLogLevelLinstor(@Nullable String linstorLogLevelRef)
    {
        if (linstorLogLevelRef != null)
        {
            logLevelLinstor = linstorLogLevelRef;
        }
    }

    public @Nullable String getConfigDir()
    {
        return configDir;
    }

    public @Nullable Path getConfigPath()
    {
        return configPath;
    }

    public boolean isDebugConsoleEnabled()
    {
        return debugConsoleEnable;
    }

    public boolean isLogPrintStackTrace()
    {
        return logPrintStackTrace;
    }

    public @Nullable String getLogDirectory()
    {
        return logDirectory;
    }

    public @Nullable String getLogLevel()
    {
        return logLevel;
    }

    public @Nullable String getLogLevelLinstor()
    {
        return logLevelLinstor;
    }

}
