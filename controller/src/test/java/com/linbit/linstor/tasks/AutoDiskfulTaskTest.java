package com.linbit.linstor.tasks;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AutoDiskfulTaskTest
{
    @Test
    public void unsetDoesNotToggle()
    {
        assertThat(AutoDiskfulTask.getToggleDiskAfterMs(null)).isNull();
    }

    @Test
    public void minusOneDisables()
    {
        assertThat(AutoDiskfulTask.getToggleDiskAfterMs("-1")).isNull();
        assertThat(AutoDiskfulTask.getToggleDiskAfterMs(Long.toString(AutoDiskfulTask.AUTO_DISKFUL_DISABLED)))
            .isNull();
    }

    @Test
    public void minutesAreConvertedToMs()
    {
        assertThat(AutoDiskfulTask.getToggleDiskAfterMs("0")).isZero();
        assertThat(AutoDiskfulTask.getToggleDiskAfterMs("1")).isEqualTo(TimeUnit.MINUTES.toMillis(1));
        assertThat(AutoDiskfulTask.getToggleDiskAfterMs("2")).isEqualTo(TimeUnit.MINUTES.toMillis(2));
    }
}
