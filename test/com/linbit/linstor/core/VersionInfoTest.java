package com.linbit.linstor.core;

import org.junit.Assert;
import org.junit.Test;

public class VersionInfoTest
{
    @Test
    public void versionInfoUnknown()
    {
        Assert.assertArrayEquals(new int[3], VersionInfoProvider.parseVersion("<Unknown>"));
    }

    @Test
    public void versionInfo()
    {
        Assert.assertArrayEquals(new int[] {0, 2, 1}, VersionInfoProvider.parseVersion("0.2.1.26-ff9ab670"));
        Assert.assertArrayEquals(new int[] {1, 0, 0}, VersionInfoProvider.parseVersion("1.0.0"));
        Assert.assertArrayEquals(new int[] {1, 0, 1}, VersionInfoProvider.parseVersion("1.0.1"));
        Assert.assertArrayEquals(new int[] {0, 9, 9}, VersionInfoProvider.parseVersion("0.9.9.1-dafkseij"));

        // should be 0.1.1
        Assert.assertArrayEquals(new int[] {0, 0, 0}, VersionInfoProvider.parseVersion("0.1"));
    }
}
