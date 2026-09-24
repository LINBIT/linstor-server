package com.linbit.linstor.core.cfg;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.Nullable;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

public abstract class LinstorConfig
{
    public static final String LINSTOR_CTRL_CONFIG = "linstor.toml";
    public static final String LINSTOR_STLT_CONFIG = "linstor_satellite.toml";

    public static final String LINSTOR_CTRL_INCLUDE_DIR = "controller.d";
    public static final String LINSTOR_STLT_INCLUDE_DIR = "satellite.d";

    public enum RestAccessLogMode
    {
        APPEND, ROTATE_HOURLY, ROTATE_DAILY, NO_LOG;
    }

    protected @Nullable String configDir;
    protected @Nullable Path configPath;

    /*
     * Additional configuration files
     */
    protected @Nullable String includeDir;
    protected @Nullable String includeDirCmdLine;
    protected @Nullable String includeDirEnv;
    protected List<Path> loadedConfigFiles = Collections.emptyList();

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
     *
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

    /**
     * Reads the main configuration file and all drop-in files of the include directory.
     *
     * <p>
     * The returned configurations have to be applied in the given order, so that a later file overrides the values
     * of an earlier one - except for the external file whitelist, which every file adds to. Parse errors are fatal,
     * just as they are for the main configuration file alone.
     * </p>
     */
    protected <T> List<T> loadTomlConfigs(String cfgFileNameRef, String dfltIncludeDirRef, Class<T> tomlClassRef)
    {
        Path mainCfgPath = Paths.get(configDir, cfgFileNameRef).normalize();
        List<T> ret = Collections.emptyList();
        try
        {
            TomlConfigLoader.Result<T> result = TomlConfigLoader.loadAll(
                mainCfgPath,
                tomlClassRef,
                includeDirCmdLine,
                includeDirEnv,
                dfltIncludeDirRef
            );
            includeDir = result.includeDir();
            loadedConfigFiles = result.paths();
            ret = result.configs();
        }
        catch (LinStorRuntimeException exc)
        {
            System.err.println(exc.getMessage());
            System.exit(InternalApiConsts.EXIT_CODE_CONFIG_PARSE_ERROR);
        }
        return ret;
    }

    public void setConfigDir(@Nullable String configDirRef)
    {
        if (configDirRef != null)
        {
            configDir = configDirRef;
            configPath = Paths.get(configDir);
        }
    }

    /**
     * The include directory given on the command line. It outranks the one of the main configuration file.
     */
    public void setIncludeDir(@Nullable String includeDirRef)
    {
        if (includeDirRef != null)
        {
            includeDirCmdLine = includeDirRef;
        }
    }

    /**
     * The include directory given in the environment. The main configuration file can override it, just as it can
     * override every other environment value.
     */
    public void setEnvIncludeDir(@Nullable String includeDirRef)
    {
        if (includeDirRef != null)
        {
            includeDirEnv = includeDirRef;
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

    /**
     * The include directory that was actually used, available once the toml files have been read.
     */
    public @Nullable String getIncludeDir()
    {
        return includeDir;
    }

    /**
     * All configuration files that were read, in the order they were applied.
     */
    public List<Path> getLoadedConfigFiles()
    {
        return loadedConfigFiles;
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
