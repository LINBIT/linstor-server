package com.linbit.linstor.core;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.Platform;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.moandjiezana.toml.Toml;

/**
 * Reloads drbd-reactor after the satellite wrote or deleted one of its snippets, so that drbd-reactor starts new
 * plugins, stops removed ones and restarts changed ones. Without this, drbd-reactor only notices the change if the
 * optional drbd-reactor-reload.path unit is enabled.
 *
 * <p>Linux only: on Windows there is no systemd unit to reload.</p>
 */
@Singleton
public class DrbdReactorSnippetReloader
{
    static final Path DEFAULT_MAIN_CONFIG = Paths.get("/etc/drbd-reactor.toml");
    static final Path DEFAULT_SNIPPETS_DIR = Paths.get("/etc/drbd-reactor.d");
    private static final String SNIPPET_SUFFIX = ".toml";
    private static final String[] RELOAD_CMD = {"systemctl", "reload", "drbd-reactor"};

    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;
    private final Path mainConfig;
    private final boolean enabled;

    private boolean snippetsChanged = false;

    @Inject
    public DrbdReactorSnippetReloader(ErrorReporter errorReporterRef, ExtCmdFactory extCmdFactoryRef)
    {
        this(errorReporterRef, extCmdFactoryRef, DEFAULT_MAIN_CONFIG, Platform.isLinux());
    }

    DrbdReactorSnippetReloader(ErrorReporter errorReporterRef, ExtCmdFactory extCmdFactoryRef, Path mainConfigRef)
    {
        this(errorReporterRef, extCmdFactoryRef, mainConfigRef, true);
    }

    private DrbdReactorSnippetReloader(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        Path mainConfigRef,
        boolean enabledRef
    )
    {
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        mainConfig = mainConfigRef;
        enabled = enabledRef;
    }

    /**
     * Checks whether drbd-reactor reads the given file, which is the case for every *.toml file directly in its
     * snippets directory (the "snippets" setting of the main configuration, /etc/drbd-reactor.d by default).
     */
    public boolean isSnippet(Path path)
    {
        @Nullable Path fileName = path.getFileName();
        return enabled && fileName != null && fileName.toString().endsWith(SNIPPET_SUFFIX) &&
            getSnippetsDir().equals(path.toAbsolutePath().normalize().getParent());
    }

    /**
     * Remembers that the given snippet was written or deleted. drbd-reactor is reloaded by the next call of
     * {@link #reloadIfSnippetsChanged()}, once for all snippets changed until then.
     */
    public void snippetChanged(Path snippet)
    {
        errorReporter.logDebug("drbd-reactor snippet %s changed, drbd-reactor will be reloaded", snippet);
        snippetsChanged = true;
    }

    /**
     * Reloads drbd-reactor if a snippet changed since the last call. A failed reload (e.g. because drbd-reactor is
     * not running) is only logged: the snippet is written, and drbd-reactor reads it when it is started.
     */
    public void reloadIfSnippetsChanged()
    {
        if (snippetsChanged)
        {
            snippetsChanged = false;
            try
            {
                ExtCmd.OutputData outputData = extCmdFactory.create().exec(RELOAD_CMD);
                if (outputData.exitCode == 0)
                {
                    errorReporter.logInfo("Reloaded drbd-reactor after its snippets changed");
                }
                else
                {
                    errorReporter.logWarning(
                        "Could not reload drbd-reactor after its snippets changed: %s",
                        new String(outputData.stderrData, StandardCharsets.UTF_8).trim()
                    );
                }
            }
            catch (IOException | ChildProcessTimeoutException exc)
            {
                errorReporter.logWarning("Could not reload drbd-reactor after its snippets changed: %s", exc);
            }
        }
    }

    private Path getSnippetsDir()
    {
        Path snippetsDir = DEFAULT_SNIPPETS_DIR;
        if (Files.isReadable(mainConfig))
        {
            try
            {
                @Nullable String configured = new Toml().read(mainConfig.toFile()).getString("snippets");
                if (configured != null && !configured.isBlank())
                {
                    snippetsDir = Paths.get(configured);
                }
            }
            catch (RuntimeException exc)
            {
                errorReporter.logWarning(
                    "Could not read the snippets directory from %s, assuming %s: %s",
                    mainConfig,
                    DEFAULT_SNIPPETS_DIR,
                    exc.getMessage()
                );
            }
        }
        return snippetsDir.toAbsolutePath().normalize();
    }
}
