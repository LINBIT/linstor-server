package com.linbit.linstor.core;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

public class LinStorCmdlArguments
{
    public static final String LS_CONFIG_DIRECTORY = "LS_CONFIG_DIRECTORY";
    public static final String LS_LOG_DIRECTORY = "LS_LOG_DIRECTORY";
    public static final String LS_LOG_LEVEL = "LS_LOG_LEVEL";

    private String configurationDirectory;
    private boolean startDebugConsole;
    private boolean printStacktraces;
    private String logDirectory;
    private String logLevel;
    private boolean toggleDbStartupVerification;

    public LinStorCmdlArguments()
    {
        configurationDirectory = getEnv(LS_CONFIG_DIRECTORY, Function.identity(), "");
        printStacktraces = false;
        logDirectory = getEnv(LS_LOG_DIRECTORY, Function.identity(), ".");
        logLevel = getEnv(LS_LOG_LEVEL, Function.identity());
        toggleDbStartupVerification = false;
    }

    public void setConfigurationDirectory(final String workingDirectoryRef)
    {
        configurationDirectory = workingDirectoryRef;
    }

    /**
     * Returns the absolute config directory path.
     * @return Path to the configuration directory
     */
    public Path getConfigurationDirectory()
    {
        return Paths.get(configurationDirectory).toAbsolutePath();
    }

    public void setStartDebugConsole(final boolean startDebugConsoleRef)
    {
        startDebugConsole = startDebugConsoleRef;
    }

    public boolean startDebugConsole()
    {
        return startDebugConsole;
    }

    public boolean isPrintStacktraces()
    {
        return printStacktraces;
    }

    public void setPrintStacktraces(boolean printStacktracesRef)
    {
        printStacktraces = printStacktracesRef;
    }

    public String getLogDirectory()
    {
        return logDirectory;
    }

    public void setLogDirectory(String newLogDirectory)
    {
        logDirectory = newLogDirectory;
    }

    public String getLogLevel()
    {
        return logLevel;
    }

    public void setLogLevel(String logLevelRef)
    {
        logLevel = logLevelRef;
    }

    public void setToggleDbStartupVerification(boolean toggleDbStartupVerificationRef)
    {
        toggleDbStartupVerification = toggleDbStartupVerificationRef;
    }

    public boolean isToggleDbStartupVerification()
    {
        return toggleDbStartupVerification;
    }

    protected <T> T getEnv(String env, Function<String, T> func)
    {
        return getEnv(env, func, null);
    }

    protected <T> T getEnv(String env, Function<String, T> func, T dfltValue)
    {
        String envVal = System.getenv(env);
        return envVal == null ? dfltValue : func.apply(envVal);
    }
}
