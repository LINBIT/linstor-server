package com.linbit.linstor.storage;

import java.math.BigDecimal;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StorageUtilsTest
{
    @Test
    public void testPoint()
    {
        assertThat(StorageUtils.parseDecimal("123.456")).isEqualTo(BigDecimal.valueOf(123456, 3));
    }

    @Test
    public void testComma()
    {
        assertThat(StorageUtils.parseDecimal("123,456")).isEqualTo(BigDecimal.valueOf(123456, 3));
    }

    @Test
    public void testInteger()
    {
        assertThat(StorageUtils.parseDecimal("123")).isEqualTo(BigDecimal.valueOf(123L));
    }

    @Test
    public void testLongPoint()
    {
        assertThat(StorageUtils.parseDecimalAsLong("123.456")).isEqualTo(123L);
    }

    @Test
    public void testLongComma()
    {
        assertThat(StorageUtils.parseDecimalAsLong("123,456")).isEqualTo(123L);
    }

    @Test
    public void testLongInteger()
    {
        assertThat(StorageUtils.parseDecimalAsLong("123")).isEqualTo(123L);
    }

    @Test
    public void testLongFloor()
    {
        assertThat(StorageUtils.parseDecimalAsLong("123.9")).isEqualTo(123L);
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    @Test(expected = NumberFormatException.class)
    public void testMultiPointFail()
    {
        StorageUtils.parseDecimal("123,456.789");
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    @Test(expected = NumberFormatException.class)
    public void testPaddedFail()
    {
        StorageUtils.parseDecimal(" 123.456");
    }
}
