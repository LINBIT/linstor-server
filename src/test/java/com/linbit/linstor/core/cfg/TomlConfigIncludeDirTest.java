package com.linbit.linstor.core.cfg;

import com.linbit.linstor.LinStorRuntimeException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TomlConfigIncludeDirTest
{
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private File cfgDir;

    @Before
    public void setUp() throws IOException
    {
        cfgDir = tmpFolder.newFolder("etc-linstor");
    }

    private void write(String relPath, String content) throws IOException
    {
        Path path = cfgDir.toPath().resolve(relPath);
        Files.createDirectories(path.getParent());
        Files.write(path, content.getBytes(StandardCharsets.UTF_8));
    }

    private StltConfig stltConfig()
    {
        return new StltConfig(new String[] {"-c", cfgDir.getAbsolutePath()});
    }

    private CtrlConfig ctrlConfig(String... additionalArgsRef)
    {
        String[] args = new String[additionalArgsRef.length + 2];
        args[0] = "-c";
        args[1] = cfgDir.getAbsolutePath();
        System.arraycopy(additionalArgsRef, 0, args, 2, additionalArgsRef.length);
        return new CtrlConfig(args);
    }

    private List<String> loadedFileNames(LinstorConfig cfgRef)
    {
        return cfgRef.getLoadedConfigFiles().stream()
            .map(path -> path.getFileName().toString())
            .collect(Collectors.toList());
    }

    @Test
    public void mainConfigOnlyStillWorks() throws IOException
    {
        write(LinstorConfig.LINSTOR_STLT_CONFIG, "[netcom]\nport = 4000\n");

        StltConfig cfg = stltConfig();

        assertEquals(Integer.valueOf(4000), cfg.getNetPort());
        assertEquals(LinstorConfig.LINSTOR_STLT_INCLUDE_DIR, cfg.getIncludeDir());
        assertEquals(List.of(LinstorConfig.LINSTOR_STLT_CONFIG), loadedFileNames(cfg));
    }

    @Test
    public void includeDirIsIgnoredWhenMissing() throws IOException
    {
        write(LinstorConfig.LINSTOR_STLT_CONFIG, "includeDir = \"elsewhere.d\"\n[netcom]\nport = 4000\n");

        StltConfig cfg = stltConfig();

        assertEquals(Integer.valueOf(4000), cfg.getNetPort());
        assertEquals(List.of(LinstorConfig.LINSTOR_STLT_CONFIG), loadedFileNames(cfg));
    }

    @Test
    public void dfltIncludeDirIsUsedWithoutBeingConfigured() throws IOException
    {
        write(LinstorConfig.LINSTOR_STLT_CONFIG, "[netcom]\nport = 4000\n");
        write(LinstorConfig.LINSTOR_STLT_INCLUDE_DIR + "/10-port.toml", "[netcom]\nport = 4001\n");

        StltConfig stltCfg = stltConfig();

        assertEquals(LinstorConfig.LINSTOR_STLT_INCLUDE_DIR, stltCfg.getIncludeDir());
        assertEquals(Integer.valueOf(4001), stltCfg.getNetPort());

        write(LinstorConfig.LINSTOR_CTRL_CONFIG, "[http]\nport = 4370\n");
        write(LinstorConfig.LINSTOR_CTRL_INCLUDE_DIR + "/10-http.toml", "[http]\nport = 4371\n");

        CtrlConfig ctrlCfg = ctrlConfig();

        assertEquals(LinstorConfig.LINSTOR_CTRL_INCLUDE_DIR, ctrlCfg.getIncludeDir());
        assertEquals(Integer.valueOf(4371), ctrlCfg.getRestBindPort());
    }

    @Test
    public void configuredIncludeDirReplacesTheDfltOne() throws IOException
    {
        write(LinstorConfig.LINSTOR_STLT_CONFIG, "includeDir = \"custom.d\"\n[netcom]\nport = 4000\n");
        write(LinstorConfig.LINSTOR_STLT_INCLUDE_DIR + "/10-port.toml", "[netcom]\nport = 4001\n");
        write("custom.d/10-port.toml", "[netcom]\nport = 4002\n");

        StltConfig cfg = stltConfig();

        assertEquals("custom.d", cfg.getIncludeDir());
        assertEquals(Integer.valueOf(4002), cfg.getNetPort());
        assertEquals(List.of(LinstorConfig.LINSTOR_STLT_CONFIG, "10-port.toml"), loadedFileNames(cfg));
    }

    @Test
    public void mainConfigIsNotReadTwiceWhenIncludedByItsOwnDirectory() throws IOException
    {
        write(LinstorConfig.LINSTOR_STLT_CONFIG, "includeDir = \".\"\n[netcom]\nport = 4000\n");
        write("10-port.toml", "[netcom]\nport = 4001\n");

        StltConfig cfg = stltConfig();

        assertEquals(List.of(LinstorConfig.LINSTOR_STLT_CONFIG, "10-port.toml"), loadedFileNames(cfg));
        assertEquals(Integer.valueOf(4001), cfg.getNetPort());
    }

    @Test
    public void laterFilesOverrideEarlierOnes() throws IOException
    {
        write(
            LinstorConfig.LINSTOR_STLT_CONFIG,
            "includeDir = \"satellite.d\"\n[netcom]\ntype = \"plain\"\nport = 4000\n"
        );
        write("satellite.d/20-port.toml", "[netcom]\nport = 4001\n");
        write("satellite.d/10-ssl.toml", "[netcom]\ntype = \"ssl\"\nport = 4002\n");
        write("satellite.d/30-logging.toml", "[logging]\nlevel = \"TRACE\"\n");
        // not matching the *.toml glob
        write("satellite.d/99-ignored.conf", "[netcom]\nport = 4099\n");

        StltConfig cfg = stltConfig();

        assertEquals(
            List.of(LinstorConfig.LINSTOR_STLT_CONFIG, "10-ssl.toml", "20-port.toml", "30-logging.toml"),
            loadedFileNames(cfg)
        );
        assertEquals("ssl", cfg.getNetType());
        assertEquals(Integer.valueOf(4001), cfg.getNetPort());
        assertEquals("TRACE", cfg.getLogLevel());
    }

    @Test
    public void externalFileWhitelistIsAddedToInsteadOfReplaced() throws IOException
    {
        write(
            LinstorConfig.LINSTOR_STLT_CONFIG,
            "[files]\nallowExtFiles = [\"/etc/drbd.d\"]\n"
        );
        write(LinstorConfig.LINSTOR_STLT_INCLUDE_DIR + "/50-gateway.toml",
            "[files]\nallowExtFiles = [\"/etc/drbd-reactor.d\"]\n");
        write(LinstorConfig.LINSTOR_STLT_INCLUDE_DIR + "/60-other.toml",
            "[files]\nallowExtFiles = [\"/etc/systemd/system\"]\n");

        StltConfig cfg = stltConfig();

        assertEquals(
            Set.of(
                Paths.get("/etc/drbd.d"),
                Paths.get("/etc/drbd-reactor.d"),
                Paths.get("/etc/systemd/system")
            ),
            cfg.getWhitelistedExternalFilePaths()
        );
    }

    @Test
    public void unsetKeysKeepTheValueOfThePreviousFile() throws IOException
    {
        write(
            LinstorConfig.LINSTOR_CTRL_CONFIG,
            "includeDir = \"linstor.d\"\n[db]\nconnection_url = \"jdbc:h2:/tmp/linstor-test-db\"\n" +
                "user = \"linstor\"\npassword = \"secret\"\n"
        );
        write("linstor.d/50-user.toml", "[db]\nuser = \"other\"\n[http]\nport = 4370\n");

        CtrlConfig cfg = ctrlConfig();

        assertEquals("jdbc:h2:/tmp/linstor-test-db", cfg.getDbConnectionUrl());
        assertEquals("other", cfg.getDbUser());
        assertEquals("secret", cfg.getDbPassword());
        assertEquals(Integer.valueOf(4370), cfg.getRestBindPort());
    }

    @Test
    public void cmdLineIncludeDirOverridesTheOneOfTheMainConfig() throws IOException
    {
        write(LinstorConfig.LINSTOR_CTRL_CONFIG, "includeDir = \"from-toml\"\n[http]\nport = 4370\n");
        write("from-toml/10-http.toml", "[http]\nport = 4371\n");
        write("from-cmdline/10-http.toml", "[http]\nport = 4372\n");

        CtrlConfig cfg = ctrlConfig("--include-directory", "from-cmdline");

        assertEquals("from-cmdline", cfg.getIncludeDir());
        assertEquals(Integer.valueOf(4372), cfg.getRestBindPort());
    }

    @Test
    public void mainConfigIncludeDirBeatsTheEnvironmentOne() throws IOException
    {
        write(LinstorConfig.LINSTOR_CTRL_CONFIG, "includeDir = \"from-toml\"\n[http]\nport = 4370\n");
        write("from-toml/10-http.toml", "[http]\nport = 4371\n");
        write("from-env/10-http.toml", "[http]\nport = 4372\n");

        CtrlConfig cfg = new CtrlConfig(new String[] {"-c", cfgDir.getAbsolutePath()})
        {
            @Override
            protected void applyEnvVars()
            {
                setEnvIncludeDir("from-env");
            }
        };

        assertEquals("from-toml", cfg.getIncludeDir());
        assertEquals(Integer.valueOf(4371), cfg.getRestBindPort());
    }

    @Test
    public void environmentIncludeDirIsUsedWithoutOneInTheMainConfig() throws IOException
    {
        write(LinstorConfig.LINSTOR_CTRL_CONFIG, "[http]\nport = 4370\n");
        write("from-env/10-http.toml", "[http]\nport = 4372\n");

        CtrlConfig cfg = new CtrlConfig(new String[] {"-c", cfgDir.getAbsolutePath()})
        {
            @Override
            protected void applyEnvVars()
            {
                setEnvIncludeDir("from-env");
            }
        };

        assertEquals("from-env", cfg.getIncludeDir());
        assertEquals(Integer.valueOf(4372), cfg.getRestBindPort());
    }

    @Test
    public void nonStringIncludeDirIsReportedAsAParseError() throws IOException
    {
        write(LinstorConfig.LINSTOR_STLT_CONFIG, "includeDir = 5\n[netcom]\nport = 4000\n");

        LinStorRuntimeException exc = assertThrows(
            LinStorRuntimeException.class,
            () -> TomlConfigLoader.loadAll(
                cfgDir.toPath().resolve(LinstorConfig.LINSTOR_STLT_CONFIG),
                StltTomlConfig.class,
                null,
                null,
                LinstorConfig.LINSTOR_STLT_INCLUDE_DIR
            )
        );
        assertTrue(exc.getMessage().contains("must be a string"));
    }

    @Test
    public void absoluteIncludeDirIsUsedAsIs() throws IOException
    {
        File otherDir = tmpFolder.newFolder("drop-ins");
        Files.write(
            otherDir.toPath().resolve("10-http.toml"),
            "[http]\nport = 4373\n".getBytes(StandardCharsets.UTF_8)
        );
        write(LinstorConfig.LINSTOR_CTRL_CONFIG, "includeDir = \"" + otherDir.getAbsolutePath() + "\"\n");

        CtrlConfig cfg = ctrlConfig();

        assertEquals(Integer.valueOf(4373), cfg.getRestBindPort());
    }

    @Test
    public void includeDirWithoutMainConfigIsRead() throws IOException
    {
        write("drop-ins/10-http.toml", "[http]\nport = 4374\n");

        CtrlConfig cfg = ctrlConfig("--include-directory", "drop-ins");

        assertEquals(Integer.valueOf(4374), cfg.getRestBindPort());
        assertEquals(List.of("10-http.toml"), loadedFileNames(cfg));
    }
}
