package com.linbit.linstor.storage;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;

import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.DRBD;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.LUKS;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.STORAGE;

public class LayerUtilsTest
{
    @Test
    public void test()
    {
        assertTrue(check(STORAGE));
        assertTrue(check(DRBD, STORAGE));
        assertTrue(check(LUKS, STORAGE));
        assertTrue(check(DRBD, LUKS, STORAGE));

        assertFalse(check());
        assertFalse(check(DRBD));
        assertFalse(check(LUKS));
        assertFalse(check(DRBD, DRBD));
        assertFalse(check(DRBD, LUKS));
        assertFalse(check(LUKS, DRBD));
        assertFalse(check(LUKS, LUKS));
        assertFalse(check(STORAGE, DRBD));
        assertFalse(check(STORAGE, LUKS));
        assertFalse(check(STORAGE, STORAGE));
        assertFalse(check(DRBD, STORAGE, LUKS));
        assertFalse(check(STORAGE, DRBD, LUKS));
        assertFalse(check(STORAGE, LUKS, DRBD));
        assertFalse(check(LUKS, STORAGE, DRBD));
    }

    private boolean check(DeviceLayerKind... kinds)
    {
        return LayerUtils.isLayerKindStackAllowed(Arrays.asList(kinds));
    }
}
