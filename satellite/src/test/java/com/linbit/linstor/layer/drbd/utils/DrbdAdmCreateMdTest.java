package com.linbit.linstor.layer.drbd.utils;

import com.linbit.linstor.storage.kinds.ExtToolsInfo;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The drbdadm arguments of {@code create-md}.
 */
@SuppressWarnings("checkstyle:magicnumber")
public class DrbdAdmCreateMdTest
{
    @Test
    public void newVolume()
    {
        assertThat(DrbdAdm.createMdArgs(new ExtToolsInfo.Version(9, 36, 0), 7, 4096, false))
            .containsExactly("--max-peers", "7", "--bitmap-block-size", "4096", "--", "--force", "create-md");
    }

    @Test
    public void volumeWithData()
    {
        // the bitmap marks the data as written, not as never written
        assertThat(DrbdAdm.createMdArgs(new ExtToolsInfo.Version(9, 36, 0), 7, 4096, true))
            .containsExactly(
                "--max-peers",
                "7",
                "--bitmap-block-size",
                "4096",
                "--",
                "--force",
                "--initialize-bitmap=set-all",
                "create-md"
            );
    }

    @Test
    public void volumeWithDataOldUtils()
    {
        // drbdadm before 9.36.0 does not pass --initialize-bitmap on, and there are no never written extents either
        assertThat(DrbdAdm.createMdArgs(new ExtToolsInfo.Version(9, 35, 0), 7, 4096, true))
            .containsExactly("--max-peers", "7", "--bitmap-block-size", "4096", "--", "--force", "create-md");
    }
}
