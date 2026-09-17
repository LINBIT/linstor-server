package com.linbit.linstor.core.cfg;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StltEnvParserTest
{
    @Test
    public void noRuntimeDirectoryKeepsTheDefault()
    {
        assertNull(StltEnvParser.clientConfFileIn(null));
        assertNull(StltEnvParser.clientConfFileIn(""));
    }

    @Test
    public void usesTheRuntimeDirectory()
    {
        assertEquals("/run/linstor/linstor-client.conf", StltEnvParser.clientConfFileIn("/run/linstor"));
    }

    @Test
    public void usesTheFirstOfSeveralRuntimeDirectories()
    {
        assertEquals(
            "/run/linstor/linstor-client.conf",
            StltEnvParser.clientConfFileIn("/run/linstor:/run/linstor-other")
        );
    }

    @Test
    public void ignoresABlankFirstEntry()
    {
        assertNull(StltEnvParser.clientConfFileIn(":/run/linstor"));
    }
}
