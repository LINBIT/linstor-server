package com.linbit.linstor.stateflags;

import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.utils.PairNonNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FlagsHelperTest
{
    @Test
    public void testToStringList()
    {
        assertThat(FlagsHelper.toStringList(TestFlags.class, 0L)).isEmpty();
        assertThat(FlagsHelper.toStringList(TestFlags.class, 1L)).containsExactly("FIRST");
        assertThat(FlagsHelper.toStringList(TestFlags.class, 3L)).containsExactly("FIRST", "SECOND");
        // COMBINED (6) is reported as soon as all of its bits are set
        assertThat(FlagsHelper.toStringList(TestFlags.class, 6L)).containsExactly("SECOND", "THIRD", "COMBINED");
        assertThat(FlagsHelper.toStringList(TestFlags.class, 7L))
            .containsExactly("FIRST", "SECOND", "THIRD", "COMBINED");
        // Unknown bits are ignored
        assertThat(FlagsHelper.toStringList(TestFlags.class, 8L)).isEmpty();
        assertThat(FlagsHelper.toStringList(TestFlags.class, 9L)).containsExactly("FIRST");
    }

    @Test
    public void testFromStringList()
    {
        assertEquals(0L, FlagsHelper.fromStringList(TestFlags.class, Collections.emptyList()));
        assertEquals(1L, FlagsHelper.fromStringList(TestFlags.class, Arrays.asList("FIRST")));
        assertEquals(5L, FlagsHelper.fromStringList(TestFlags.class, Arrays.asList("FIRST", "THIRD")));
        assertEquals(6L, FlagsHelper.fromStringList(TestFlags.class, Arrays.asList("COMBINED")));
        assertEquals(1L, FlagsHelper.fromStringList(TestFlags.class, Arrays.asList("FIRST", "FIRST")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromStringListUnknownFlag()
    {
        FlagsHelper.fromStringList(TestFlags.class, Arrays.asList("BOGUS"));
    }

    @Test
    public void testToFlagsArray()
    {
        @SuppressWarnings("unchecked")
        StateFlags<TestFlags> stateFlags = Mockito.mock(StateFlags.class);
        Mockito.when(stateFlags.isSet(TestFlags.FIRST)).thenReturn(true);
        Mockito.when(stateFlags.isSet(TestFlags.THIRD)).thenReturn(true);

        TestFlags[] result = FlagsHelper.toFlagsArray(TestFlags.class, stateFlags);
        assertArrayEquals(new TestFlags[] {TestFlags.FIRST, TestFlags.THIRD}, result);
    }

    @Test
    public void testToFlagsArrayNoneSet()
    {
        @SuppressWarnings("unchecked")
        StateFlags<TestFlags> stateFlags = Mockito.mock(StateFlags.class);

        assertEquals(0, FlagsHelper.toFlagsArray(TestFlags.class, stateFlags).length);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testIsFlagEnabled()
    {
        assertTrue(FlagsHelper.isFlagEnabled(5L, TestFlags.FIRST));
        assertTrue(FlagsHelper.isFlagEnabled(5L, TestFlags.THIRD));
        assertTrue(FlagsHelper.isFlagEnabled(5L, TestFlags.FIRST, TestFlags.THIRD));
        assertFalse(FlagsHelper.isFlagEnabled(5L, TestFlags.SECOND));
        assertFalse(FlagsHelper.isFlagEnabled(5L, TestFlags.FIRST, TestFlags.SECOND));
        assertTrue(FlagsHelper.isFlagEnabled(6L, TestFlags.COMBINED));
        assertFalse(FlagsHelper.isFlagEnabled(2L, TestFlags.COMBINED));
        // Without any flags to check, the result is vacuously true
        assertTrue(FlagsHelper.<TestFlags>isFlagEnabled(0L));
    }

    @Test
    public void testExtractFlagsToEnableOrDisable()
    {
        PairNonNull<Set<TestFlags>, Set<TestFlags>> result = FlagsHelper.extractFlagsToEnableOrDisable(
            TestFlags.class,
            Arrays.asList("FIRST", "-SECOND", "THIRD")
        );
        assertThat(result.objA).containsExactly(TestFlags.FIRST, TestFlags.THIRD);
        assertThat(result.objB).containsExactly(TestFlags.SECOND);
    }

    @Test
    public void testExtractFlagsToEnableOrDisableEmptyList()
    {
        PairNonNull<Set<TestFlags>, Set<TestFlags>> result = FlagsHelper.extractFlagsToEnableOrDisable(
            TestFlags.class,
            Collections.emptyList()
        );
        assertThat(result.objA).isEmpty();
        assertThat(result.objB).isEmpty();
    }

    @Test
    public void testExtractFlagsToEnableOrDisableSameFlagInBothSets()
    {
        // The same flag can end up in both the enable and the disable set
        PairNonNull<Set<TestFlags>, Set<TestFlags>> result = FlagsHelper.extractFlagsToEnableOrDisable(
            TestFlags.class,
            Arrays.asList("FIRST", "-FIRST")
        );
        assertThat(result.objA).containsExactly(TestFlags.FIRST);
        assertThat(result.objB).containsExactly(TestFlags.FIRST);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractFlagsToEnableOrDisableUnknownFlag()
    {
        FlagsHelper.extractFlagsToEnableOrDisable(TestFlags.class, Arrays.asList("-BOGUS"));
    }

    @Test
    public void testFlagRenamedWithinServer()
    {
        // the API still calls ResourceDefinition.Flags.PREVENT_RESOURCE_CREATION RESTORE_TARGET
        long bits = ResourceDefinition.Flags.PREVENT_RESOURCE_CREATION.getFlagValue();
        assertThat(FlagsHelper.toStringList(ResourceDefinition.Flags.class, bits)).containsExactly("RESTORE_TARGET");
        assertEquals(bits, FlagsHelper.fromStringList(ResourceDefinition.Flags.class, Arrays.asList("RESTORE_TARGET")));
        assertThat(
            FlagsHelper.extractFlagsToEnableOrDisable(ResourceDefinition.Flags.class, Arrays.asList("-RESTORE_TARGET"))
                .objB
        ).containsExactly(ResourceDefinition.Flags.PREVENT_RESOURCE_CREATION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlagRenamedWithinServerInternalName()
    {
        FlagsHelper.fromStringList(ResourceDefinition.Flags.class, Arrays.asList("PREVENT_RESOURCE_CREATION"));
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testGetBits()
    {
        assertEquals(0L, FlagsHelper.<TestFlags>getBits());
        assertEquals(1L, FlagsHelper.getBits(TestFlags.FIRST));
        assertEquals(5L, FlagsHelper.getBits(TestFlags.FIRST, TestFlags.THIRD));
        assertEquals(6L, FlagsHelper.getBits(TestFlags.SECOND, TestFlags.COMBINED));
        assertEquals(1L, FlagsHelper.getBits(TestFlags.FIRST, TestFlags.FIRST));
    }

    private enum TestFlags implements Flags
    {
        FIRST(1L),
        SECOND(1L << 1),
        THIRD(1L << 2),
        COMBINED((1L << 1) | (1L << 2));

        private final long flagValue;

        TestFlags(long flagValueRef)
        {
            flagValue = flagValueRef;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }
    }
}
