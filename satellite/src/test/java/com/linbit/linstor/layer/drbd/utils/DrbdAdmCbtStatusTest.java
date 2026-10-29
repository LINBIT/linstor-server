package com.linbit.linstor.layer.drbd.utils;

import com.linbit.linstor.layer.drbd.utils.DrbdAdm.CbtStatus;
import com.linbit.linstor.storage.StorageException;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Parsing the output of {@code drbdsetup cbt-status} (drbd-utils {@code cbt_status_cmd}).
 */
public class DrbdAdmCbtStatusTest
{
    @Test
    public void activeTracking() throws Exception
    {
        CbtStatus status = CbtStatus.parse(
            """
            {
              "id": "0123456789ABCDEF",
              "since": "2026-10-29T10:00:00Z",
              "state": "valid",
              "bitmap-uuid": "00000000000000AB",
              "dagtag": 4096
            }
            """
        );

        assertThat(status.bookmarkId()).isEqualTo("0123456789ABCDEF");
        assertThat(status.state()).isEqualTo("valid");
        assertThat(status.isActive()).isTrue();
    }

    @Test
    public void stoppedTracking() throws Exception
    {
        CbtStatus status = CbtStatus.parse("{\n  \"state\": \"stopped\"\n}\n");

        assertThat(status.bookmarkId()).isNull();
        assertThat(status.isActive()).isFalse();
    }

    @Test
    public void unexpectedOutput()
    {
        assertThatThrownBy(() -> CbtStatus.parse("cbt-status: unknown command")).isInstanceOf(StorageException.class);
        assertThatThrownBy(() -> CbtStatus.parse("{}")).isInstanceOf(StorageException.class);
    }
}
