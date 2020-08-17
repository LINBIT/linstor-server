package com.linbit.linstor;

import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExtToolsInfoTest
{
    @Test
    public void versionTest() {
        assertTrue(new Version(1, 0, 0).greaterOrEqual(new Version(null, null, null)));
        assertTrue(new Version(1, 0, 0).greaterOrEqual(new Version(1, null, null)));
        assertTrue(new Version(1, 0, 0).greaterOrEqual(new Version(1, 0, null)));
        assertTrue(new Version(1, 0, 0).greaterOrEqual(new Version(1, 0, 0)));
        assertTrue(new Version(1, 0, 0).greaterOrEqual(new Version(0, 0, 0)));
        assertTrue(new Version(1, 0, 0).greaterOrEqual(new Version(0, 9, 0)));
        assertTrue(new Version(1, 0, 0).greaterOrEqual(new Version(0, 9, 42)));
        assertTrue(new Version(1, 0, 0).greaterOrEqual(new Version(null, 9, 42)));
        assertTrue(new Version(1, 0, 0).greaterOrEqual(new Version(null, null, 42)));
        assertTrue(new Version(1, 0, 0).greaterOrEqual(new Version(1, null, 42)));

        assertTrue(new Version(1, 0, null).greaterOrEqual(new Version(1, 0, 42)));
        assertTrue(new Version(1, null, null).greaterOrEqual(new Version(1, 9, 42)));

        assertFalse(new Version(1, 0, 0).greaterOrEqual(new Version(1, 0, 1)));
        assertFalse(new Version(1, 0, 0).greaterOrEqual(new Version(1, 1, 0)));
        assertFalse(new Version(1, 0, 0).greaterOrEqual(new Version(2, 0, 0)));
        assertFalse(new Version(1, 0, null).greaterOrEqual(new Version(2, 0, 0)));
        assertFalse(new Version(1, null, null).greaterOrEqual(new Version(2, 0, 0)));
        assertFalse(new Version(1, null, 0).greaterOrEqual(new Version(2, 0, 0)));
        assertFalse(new Version(1, null, 0).greaterOrEqual(new Version(2, null, 0)));
        assertFalse(new Version(1, null, 0).greaterOrEqual(new Version(2, null, null)));
    }
}
