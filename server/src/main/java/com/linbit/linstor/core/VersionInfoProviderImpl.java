package com.linbit.linstor.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class VersionInfoProviderImpl implements VersionInfoProvider
{
    private static final String UNKNOWN_PLACEHOLDER = "<Unknown>";

    private final String version;
    private final String gitCommitId;
    private final String buildTime;

    public VersionInfoProviderImpl()
    {
        Properties props = new Properties();
        try
        (
            InputStream is = getClass().getClassLoader().getResourceAsStream("version-info.properties");
        )
        {
            if (is != null)
            {
                props.load(is);
            }
        }
        catch (IOException | IllegalArgumentException ignored)
        {
            // IllegalArgumentException can be thrown if the input stream contains
            // a malformed unicode escape sequence
        }

        version = props.getProperty("version", UNKNOWN_PLACEHOLDER);
        gitCommitId = props.getProperty("git.commit.id", UNKNOWN_PLACEHOLDER);
        buildTime = props.getProperty("build.time", UNKNOWN_PLACEHOLDER);
    }

    @Override
    public String getVersion()
    {
        return version;
    }

    @Override
    public String getGitCommitId()
    {
        return gitCommitId;
    }

    @Override
    public String getBuildTime()
    {
        return buildTime;
    }

    @Override
    public int[] getSemanticVersion()
    {
        return VersionInfoProvider.parseVersion(version);
    }

    @Override
    public boolean equalsVersion(int major, int minor, int patch)
    {
        int[] ownVersion = getSemanticVersion();
        return ownVersion[0] == major && ownVersion[1] == minor && ownVersion[2] == patch;
    }
}
