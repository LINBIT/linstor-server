package com.linbit.linstor.layer.drbd;

import com.linbit.linstor.layer.drbd.CbtSessionService.CbtExtents;
import com.linbit.linstor.layer.drbd.CbtSessionService.Extent;
import com.linbit.linstor.layer.drbd.CbtSessionService.ExtentsMode;
import com.linbit.linstor.storage.StorageException;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Parsing the output of {@code drbdmeta cbt-dump} (drbd-utils {@code cbt_dump_cmd}).
 */
@SuppressWarnings("checkstyle:magicnumber")
public class CbtSessionServiceDumpTest
{
    @Test
    public void dirtyMode() throws Exception
    {
        CbtExtents dump = CbtExtents.parse(
            """
            {
              "format": 1,
              "id": "A53B547A6589E9D2",
              "since": "2026-10-29T11:49:43Z",
              "volume-size": 423493632,
              "extents": [
                { "start": 0, "length": 1331200, "dirty": false, "zero": false },
                { "start": 1331200, "length": 4096, "dirty": true, "zero": false },
                { "start": 1335296, "length": 422158336, "dirty": false, "zero": true }
              ]
            }
            """,
            ExtentsMode.DIRTY
        );

        assertThat(dump.baseCbtId()).isEqualTo("A53B547A6589E9D2");
        assertThat(dump.since()).isEqualTo("2026-10-29T11:49:43Z");
        assertThat(dump.volumeSize()).isEqualTo(423493632L);
        assertThat(dump.extents()).containsExactly(
            new Extent(0, 1331200, false, false),
            new Extent(1331200, 4096, true, false),
            new Extent(1335296, 422158336, false, true)
        );
        assertThat(dump.nextOffset()).isNull();
    }

    @Test
    public void zeroMode() throws Exception
    {
        CbtExtents dump = CbtExtents.parse(
            """
            {
              "format": 1,
              "volume-size": 423493632,
              "extents": [
                { "start": 8192, "length": 4096, "zero": false },
                { "start": 12288, "length": 4096, "zero": true }
              ],
              "next-offset": 16384
            }
            """,
            ExtentsMode.ZERO
        );

        assertThat(dump.baseCbtId()).isNull();
        assertThat(dump.since()).isNull();
        assertThat(dump.extents()).containsExactly(
            new Extent(8192, 4096, null, false),
            new Extent(12288, 4096, null, true)
        );
        assertThat(dump.nextOffset()).isEqualTo(16384L);
    }

    @Test
    public void unexpectedOutput()
    {
        assertThatThrownBy(() -> parseDirty("not json")).isInstanceOf(StorageException.class);
        assertThatThrownBy(() -> parseDirty("{ \"format\": 1, \"id\": \"A\", \"volume-size\": 1 }"))
            .isInstanceOf(StorageException.class);
        assertThatThrownBy(
            () -> parseDirty(
                "{ \"format\": 1, \"id\": \"A\", \"volume-size\": 1, " +
                    "\"extents\": [ { \"start\": 0, \"dirty\": true, \"zero\": false } ] }"
            )
        ).isInstanceOf(StorageException.class);
        // no id in dirty mode
        assertThatThrownBy(() -> parseDirty("{ \"format\": 1, \"volume-size\": 1, \"extents\": [ ] }"))
            .isInstanceOf(StorageException.class);
        // no dirty flag in dirty mode
        assertThatThrownBy(
            () -> parseDirty(
                "{ \"format\": 1, \"id\": \"A\", \"volume-size\": 1, " +
                    "\"extents\": [ { \"start\": 0, \"length\": 1, \"zero\": true } ] }"
            )
        ).isInstanceOf(StorageException.class);
        // a format this parser does not know
        assertThatThrownBy(() -> parseDirty("{ \"format\": 2, \"id\": \"A\", \"volume-size\": 1, \"extents\": [ ] }"))
            .isInstanceOf(StorageException.class);
        // drbd-utils 9.36.0-rc.1: no format, only the dirty extents
        assertThatThrownBy(
            () -> parseDirty("{ \"id\": \"A\", \"volume-size\": 1, \"extents\": [ { \"offset\": 0, \"length\": 1 } ] }")
        ).isInstanceOf(StorageException.class);
        assertThatThrownBy(() -> CbtExtents.parse("{ \"volume-size\": 1, \"extents\": [ ] }", ExtentsMode.ZERO))
            .isInstanceOf(StorageException.class);
    }

    private static CbtExtents parseDirty(String jsonRef) throws StorageException
    {
        return CbtExtents.parse(jsonRef, ExtentsMode.DIRTY);
    }
}
