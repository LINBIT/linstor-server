package com.linbit.linstor.core;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.testutils.EmptyErrorReporter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class DrbdReactorSnippetReloaderTest
{
    private static final String[] RELOAD_CMD = {"systemctl", "reload", "drbd-reactor"};

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    private ExtCmd extCmd;
    private ExtCmdFactory extCmdFactory;

    @Before
    public void setUp() throws Exception
    {
        extCmd = Mockito.mock(ExtCmd.class);
        Mockito.when(extCmd.exec(RELOAD_CMD))
            .thenReturn(new ExtCmd.OutputData(RELOAD_CMD, new byte[0], new byte[0], 0));
        extCmdFactory = Mockito.mock(ExtCmdFactory.class);
        Mockito.when(extCmdFactory.create()).thenReturn(extCmd);
    }

    private DrbdReactorSnippetReloader reloader(Path mainConfig)
    {
        return new DrbdReactorSnippetReloader(new EmptyErrorReporter(), extCmdFactory, mainConfig);
    }

    @Test
    public void snippetsInTheDefaultDirectory()
    {
        DrbdReactorSnippetReloader reloader = reloader(tmpDir.getRoot().toPath().resolve("missing.toml"));

        assertThat(reloader.isSnippet(Paths.get("/etc/drbd-reactor.d/linstor_db.toml"))).isTrue();
        // drbd-reactor only reads *.toml, a disabled snippet does not count
        assertThat(reloader.isSnippet(Paths.get("/etc/drbd-reactor.d/linstor_db.toml.disabled"))).isFalse();
        assertThat(reloader.isSnippet(Paths.get("/etc/drbd-reactor.d/sub/linstor_db.toml"))).isFalse();
        assertThat(reloader.isSnippet(Paths.get("/etc/systemd/system/linstor_db.toml"))).isFalse();
    }

    @Test
    public void snippetsDirectoryFromTheMainConfig() throws IOException
    {
        Path mainConfig = tmpDir.newFile("drbd-reactor.toml").toPath();
        Files.write(mainConfig, "snippets = \"/srv/reactor.d\"\n".getBytes(StandardCharsets.UTF_8));
        DrbdReactorSnippetReloader reloader = reloader(mainConfig);

        assertThat(reloader.isSnippet(Paths.get("/srv/reactor.d/promoter.toml"))).isTrue();
        assertThat(reloader.isSnippet(Paths.get("/etc/drbd-reactor.d/promoter.toml"))).isFalse();
    }

    @Test
    public void reloadsOnceForSeveralChangedSnippets() throws Exception
    {
        DrbdReactorSnippetReloader reloader = reloader(tmpDir.getRoot().toPath().resolve("missing.toml"));

        reloader.snippetChanged(Paths.get("/etc/drbd-reactor.d/a.toml"));
        reloader.snippetChanged(Paths.get("/etc/drbd-reactor.d/b.toml"));
        reloader.reloadIfSnippetsChanged();
        // nothing changed since the last reload
        reloader.reloadIfSnippetsChanged();

        Mockito.verify(extCmd, Mockito.times(1)).exec(RELOAD_CMD);
    }

    @Test
    public void noReloadWithoutChangedSnippets()
    {
        reloader(tmpDir.getRoot().toPath().resolve("missing.toml")).reloadIfSnippetsChanged();

        Mockito.verifyNoInteractions(extCmdFactory);
    }

    @Test
    public void failedReloadDoesNotThrowAndIsNotRepeated() throws Exception
    {
        Mockito.when(extCmd.exec(RELOAD_CMD)).thenReturn(
            new ExtCmd.OutputData(
                RELOAD_CMD,
                new byte[0],
                "drbd-reactor.service is not active, cannot reload.".getBytes(StandardCharsets.UTF_8),
                1
            )
        );
        DrbdReactorSnippetReloader reloader = reloader(tmpDir.getRoot().toPath().resolve("missing.toml"));

        reloader.snippetChanged(Paths.get("/etc/drbd-reactor.d/a.toml"));
        reloader.reloadIfSnippetsChanged();
        reloader.reloadIfSnippetsChanged();

        Mockito.verify(extCmd, Mockito.times(1)).exec(RELOAD_CMD);
    }
}
