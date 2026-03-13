package com.linbit;

import com.linbit.SizeConv.SizeUnit;
import com.linbit.linstor.LinstorParsingException;
import com.linbit.linstor.annotation.Nullable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class SizeSpecParserTest
{
    // --- Parameterized absolute unit parsing ---
    private record AbsoluteUnitTest(
        String input,
        @Nullable SizeSpecParser.Config cfg,
        long expectedNum,
        SizeUnit expectedUnit
    )
    {
        public AbsoluteUnitTest(String input, long expectedNum, SizeUnit expectedUnit)
        {
            this(input, null, expectedNum, expectedUnit);
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
    private AbsoluteUnitTest[] absoluteUnitCases()
    {
        return new AbsoluteUnitTest[] {
            // Three-letter units (power-of-two)
            new AbsoluteUnitTest("512KiB", 512, SizeUnit.UNIT_KiB),
            new AbsoluteUnitTest("256MiB", 256, SizeUnit.UNIT_MiB),
            new AbsoluteUnitTest("100GiB", 100, SizeUnit.UNIT_GiB),
            new AbsoluteUnitTest("2TiB", 2, SizeUnit.UNIT_TiB),
            new AbsoluteUnitTest("2 TiB", 2, SizeUnit.UNIT_TiB),
            new AbsoluteUnitTest("1.5TiB", 1536, SizeUnit.UNIT_GiB),
            new AbsoluteUnitTest("0.0009746551513671875TiB", 1022, SizeUnit.UNIT_MiB),
            new AbsoluteUnitTest("0.00097560882568359375TiB", 1023, SizeUnit.UNIT_MiB),
            // Two-letter units (decimal)
            new AbsoluteUnitTest("1KB", 1, SizeUnit.UNIT_kB),
            new AbsoluteUnitTest("1MB", 1, SizeUnit.UNIT_MB),
            new AbsoluteUnitTest("10GB", 10, SizeUnit.UNIT_GB),
            new AbsoluteUnitTest("1TB", 1, SizeUnit.UNIT_TB),
            new AbsoluteUnitTest("1 TB", 1, SizeUnit.UNIT_TB),
            new AbsoluteUnitTest("1.5TB", 1500, SizeUnit.UNIT_GB),
            new AbsoluteUnitTest("0.000975609TB", 975609, SizeUnit.UNIT_kB),
            // Single-letter units (forced power-of-two)
            new AbsoluteUnitTest("1024k", 1024, SizeUnit.UNIT_KiB),
            new AbsoluteUnitTest("1K", 1, SizeUnit.UNIT_KiB),
            new AbsoluteUnitTest("64M", 64, SizeUnit.UNIT_MiB),
            new AbsoluteUnitTest("1m", 1, SizeUnit.UNIT_MiB),
            new AbsoluteUnitTest("10G", 10, SizeUnit.UNIT_GiB),
            new AbsoluteUnitTest("1g", 1, SizeUnit.UNIT_GiB),
            new AbsoluteUnitTest("4T", 4, SizeUnit.UNIT_TiB),
            new AbsoluteUnitTest("1t", 1, SizeUnit.UNIT_TiB),
            new AbsoluteUnitTest("1 t", 1, SizeUnit.UNIT_TiB),
            new AbsoluteUnitTest("1.5t", 1536, SizeUnit.UNIT_GiB),
            new AbsoluteUnitTest("0.0009746551513671875 t", 1022, SizeUnit.UNIT_MiB),
            new AbsoluteUnitTest("0.00097560882568359375 t", 1023, SizeUnit.UNIT_MiB),
            // Sectors
            new AbsoluteUnitTest("8s", 8, SizeUnit.UNIT_SECTORS),
            new AbsoluteUnitTest("16S", 16, SizeUnit.UNIT_SECTORS),
            new AbsoluteUnitTest("16 S", 16, SizeUnit.UNIT_SECTORS),
            // Edge cases
            new AbsoluteUnitTest("0GiB", 0, SizeUnit.UNIT_GiB),
            new AbsoluteUnitTest("999999GiB", 999999, SizeUnit.UNIT_GiB),
            new AbsoluteUnitTest("999999 GiB", 999999, SizeUnit.UNIT_GiB),
            // No config with default unit
            new AbsoluteUnitTest("1024", 1024, SizeUnit.UNIT_KiB),
            // config with overridden default unit
            new AbsoluteUnitTest("256", cfg().dfltSizeUnit(SizeUnit.UNIT_MiB), 256, SizeUnit.UNIT_MiB),
            // allow rounding
            new AbsoluteUnitTest("5.3GiB", cfg().allowRounding(true), 5_690_831_668L, SizeUnit.UNIT_B),
            new AbsoluteUnitTest("5.3GiB", cfg().allowRounding(true).ceil(false), 5_690_831_667L, SizeUnit.UNIT_B),
        };
    }

    // @Test
    // public void testForDebuggingFailingAbsTests() throws LinstorParsingException
    // {
    // testParseAbsoluteUnit(absoluteUnitCases()[35]);
    // }

    @Test
    @Parameters(method = "absoluteUnitCases")
    public void testParseAbsoluteUnit(AbsoluteUnitTest test)
        throws LinstorParsingException
    {
        expectAbsSize(test.input, test.cfg, test.expectedNum, test.expectedUnit);
    }

    // --- Percent parsing ---
    private record PercentUnitTest(String input, @Nullable SizeSpecParser.Config cfg, float expectedNum)
    {
        public PercentUnitTest(String input, float expectedNum)
        {
            this(input, SizeSpecParserTest.cfg().allowPercent(true), expectedNum);
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
    private PercentUnitTest[] percentCases()
    {
        return new PercentUnitTest[] {
            new PercentUnitTest("0%", 0f),
            new PercentUnitTest("50%", 50f),
            new PercentUnitTest("100%", 100f),
            new PercentUnitTest("6.5 %", 6.5f),
            new PercentUnitTest("9000.1 %", 9000.1f), // not limited to 100%
            new PercentUnitTest("75", cfg().dfltPercent(true), 75f)
        };
    }

    // @Test
    // public void testForDebuggingFailingPecentTests() throws LinstorParsingException
    // {
    // testParsePercent(percentCases()[0]);
    // }

    @Test
    @Parameters(method = "percentCases")
    public void testParsePercent(PercentUnitTest test) throws LinstorParsingException
    {
        expectPercentSize(test.input, test.cfg, test.expectedNum);
    }


    private record InvalidTest(String input, @Nullable SizeSpecParser.Config cfg)
    {
        public InvalidTest(String input)
        {
            this(input, null);
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
    private InvalidTest[] invalidCases()
    {
        return new InvalidTest[] {
            new InvalidTest("50%"), // percent is now allowed by default
            new InvalidTest("100GiB", cfg().allowAbsolute(false).allowPercent(true)),
            new InvalidTest("8s", cfg().allowSectors(false)),
            new InvalidTest("42", cfg().dfltSizeUnit(null)),
            new InvalidTest(""),
            new InvalidTest("GiB"),
            new InvalidTest("-100GiB"),
            new InvalidTest("1.5s"),
            new InvalidTest("5.3GiB"), // is still 5690831667.2 B, which is not valid (without rounding)
            new InvalidTest("what?"),
        };
    }

    @Test(expected = LinstorParsingException.class)
    @Parameters(method = "invalidCases")
    public void testInvalidInput(InvalidTest test) throws LinstorParsingException
    {
        SizeSpecParser.parse(test.input, test.cfg == null ? cfg() : test.cfg);
    }

    // --- Config builder mutual exclusion ---

    @Test
    public void testConfigDfltPercentClearsSizeUnit()
    {
        SizeSpecParser.Config cfg = new SizeSpecParser.Config()
            .dfltSizeUnit(SizeUnit.UNIT_GiB)
            .dfltPercent(true);
        assertTrue(cfg.dfltPercent());
        assertNull(cfg.dfltSizeUnit());
    }

    @Test
    public void testConfigDfltSizeUnitClearsPercent()
    {
        SizeSpecParser.Config cfg = new SizeSpecParser.Config()
            .dfltPercent(true)
            .dfltSizeUnit(SizeUnit.UNIT_GiB);
        assertFalse(cfg.dfltPercent());
        assertEquals(SizeUnit.UNIT_GiB, cfg.dfltSizeUnit());
    }

    private void expectAbsSize(
        String inputRef,
        @Nullable SizeSpecParser.Config cfgRef,
        long expectedNumRef,
        SizeUnit expectedUnitRef
    )
        throws LinstorParsingException
    {
        SizeSpec.Abs size = asAbsSpec(
            inputRef,
            SizeSpecParser.parse(inputRef, cfgRef == null ? cfg() : cfgRef)
        );
        assertEquals("num for " + inputRef, expectedNumRef, size.num());
        assertEquals("unit for " + inputRef, expectedUnitRef, size.unit());
    }

    private void expectPercentSize(String inputRef, @Nullable SizeSpecParser.Config cfgRef, float expectedNumRef)
        throws LinstorParsingException
    {
        SizeSpec.Percent size = asPercSpec(
            inputRef,
            SizeSpecParser.parse(inputRef, cfgRef == null ? cfg() : cfgRef)
        );
        assertEquals("num for " + inputRef, expectedNumRef, size.num(), 0f);
    }

    private SizeSpec.Abs asAbsSpec(String inputStringRef, SizeSpec specRef)
    {
        return checkClass(inputStringRef, specRef, SizeSpec.Abs.class);
    }

    private SizeSpec.Percent asPercSpec(String inputStringRef, SizeSpec specRef)
    {
        return checkClass(inputStringRef, specRef, SizeSpec.Percent.class);
    }

    private static SizeSpecParser.Config cfg()
    {
        return new SizeSpecParser.Config();
    }

    @SuppressWarnings("unchecked")
    private <T extends SizeSpec> T checkClass(String inputRef, SizeSpec specRef, Class<T> expectedClassRef)
    {
        assertEquals("class for " + inputRef, expectedClassRef, specRef.getClass());
        return (T) specRef;
    }
}
