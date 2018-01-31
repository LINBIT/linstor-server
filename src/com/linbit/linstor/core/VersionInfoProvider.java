package com.linbit.linstor.core;

public interface VersionInfoProvider
{
    String getVersion();

    String getGitCommitId();

    String getBuildTime();
}
