package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Arrays;
import java.util.TreeSet;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class CtrlRscDfnApiCallHelperTest
{
    private static final long KIB = 1024;
    private static final long MIB = 1024 * KIB;
    private static final long GIB = 1024 * MIB;
    private static final long DRBD_MAX = 128 * MIB;

    private static @Nullable Long choose(long blockSize, long max, Long... discGrans)
    {
        return CtrlRscDfnApiCallHelper.chooseDiscardGran(new TreeSet<>(Arrays.asList(discGrans)), blockSize, max);
    }

    @Test
    public void nothingReported()
    {
        assertThat(choose(4 * KIB, DRBD_MAX)).isNull();
        assertThat(choose(4 * KIB, DRBD_MAX, VlmProviderObject.UNINITIALIZED_SIZE)).isNull();
    }

    @Test
    public void anyZeroDisablesDiscards()
    {
        assertThat(choose(4 * KIB, DRBD_MAX, 0L, 64 * KIB)).isEqualTo(0L);
    }

    @Test
    public void largestMultipleOfBlockSizeUnchanged()
    {
        assertThat(choose(512, DRBD_MAX, 512L)).isEqualTo(512L);
        assertThat(choose(4 * KIB, DRBD_MAX, 4 * KIB, 64 * KIB)).isEqualTo(64 * KIB);
    }

    @Test
    public void roundedUpToBlockSize()
    {
        assertThat(choose(4 * KIB, DRBD_MAX, 512L)).isEqualTo(4 * KIB);
        assertThat(choose(4 * KIB, DRBD_MAX, 5 * KIB)).isEqualTo(8 * KIB);
    }

    @Test
    public void maximumUnchanged()
    {
        assertThat(choose(4 * KIB, DRBD_MAX, DRBD_MAX)).isEqualTo(DRBD_MAX);
    }

    @Test
    public void disabledAboveMaximum()
    {
        assertThat(choose(4 * KIB, DRBD_MAX, GIB)).isEqualTo(0L);
        assertThat(choose(4 * KIB, DRBD_MAX, DRBD_MAX + 1)).isEqualTo(0L);
    }

    @Test
    public void disabledIfRoundedUpAboveMaximum()
    {
        assertThat(choose(4 * KIB, 10 * KIB, 7 * KIB)).isEqualTo(8 * KIB);
        assertThat(choose(4 * KIB, 10 * KIB, 9 * KIB)).isEqualTo(0L);
    }

    @Test
    public void blockSizeAboveMaximum()
    {
        assertThat(choose(4 * KIB, 2 * KIB, 512L)).isEqualTo(0L);
    }
}
