package com.linbit.linstor.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface VersionInfoProvider
{
    String getVersion();

    String getGitCommitId();

    String getBuildTime();

    int[] getSemanticVersion();

    boolean equalsVersion(int major, int minor, int patch);

    static int[] parseVersion(String sVersion)
    {
        int[] ret = new int[3];
        if (!sVersion.equals("<Unknown>"))
        {
            Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+).*");
            Matcher matcher = pattern.matcher(sVersion);
            if (matcher.matches())
            {
                ret[0] = Integer.parseInt(matcher.group(1));
                ret[1] = Integer.parseInt(matcher.group(2));
                ret[2] = Integer.parseInt(matcher.group(3));
            }
        }

        return ret;
    }
}
