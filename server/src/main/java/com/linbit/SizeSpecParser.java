package com.linbit;

import com.linbit.SizeConv.SizeUnit;
import com.linbit.linstor.LinstorParsingException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SizeSpecParser
{
    public static final SizeSpecParser.Config PARSER_WITH_PERCENT = new SizeSpecParser.Config().allowPercent(true);

    private static final Pattern PATTERN = Pattern.compile(
        "(\\d+(?:[.]\\d+)?)" + // number group, i.e. "10", "10.4", etc...
        "\\s*" + // whitespaces... why not
        "(" + // size-unit group start
            "[kKmMgGtT](?:i[bB]|[bB])?|" + // Kib, kb
            "[bB]|" +
            "[sS]|" +
            "%|" +
            "" + //nothing is also allowed as a unit. Config decides what to default to here (% or given unit)
        ")" // size-unit group end
    );
    private static final HashMap<SizeUnit, SizeUnit> NEXT_LOWER_1024 = new HashMap<>();
    private static final HashMap<SizeUnit, SizeUnit> NEXT_LOWER_1000 = new HashMap<>();
    private static final BigInteger BIG_INT_1024 = BigInteger.valueOf(1024);
    private static final BigInteger BIG_INT_1000 = BigInteger.valueOf(1000);
    private static final int LONG_BITS = 63;

    static
    {
        SizeUnit[] order = {
            SizeUnit.UNIT_YiB,
            SizeUnit.UNIT_ZiB,
            SizeUnit.UNIT_EiB,
            SizeUnit.UNIT_PiB,
            SizeUnit.UNIT_TiB,
            SizeUnit.UNIT_GiB,
            SizeUnit.UNIT_MiB,
            SizeUnit.UNIT_KiB,
            // we skip sectors because it is rather unusual and possibly also not allowed
            SizeUnit.UNIT_B,
        };
        for (int idx = 0; idx < order.length - 1; idx++)
        {
            NEXT_LOWER_1024.put(order[idx], order[idx + 1]);
        }
        order = new SizeUnit[] {
            SizeUnit.UNIT_YB,
            SizeUnit.UNIT_ZB,
            SizeUnit.UNIT_EB,
            SizeUnit.UNIT_PB,
            SizeUnit.UNIT_TB,
            SizeUnit.UNIT_GB,
            SizeUnit.UNIT_MB,
            SizeUnit.UNIT_kB,
            // we skip sectors because it is rather unusual and possibly also not allowed
            SizeUnit.UNIT_B,
        };
        for (int idx = 0; idx < order.length - 1; idx++)
        {
            NEXT_LOWER_1000.put(order[idx], order[idx + 1]);
        }
    }

    public static class Config
    {
        private boolean allowAbsolute = true;
        private boolean allowSectors = true;
        private boolean allowPercent = false;
        private boolean allowNoUnit = false;
        private boolean allowRounding = false;
        private boolean roundCeil = true; // true -> ceil, false -> floor

        private @Nullable SizeUnit dfltSizeUnit = SizeUnit.UNIT_KiB;
        private boolean dfltPercent = false;

        public boolean allowAbsolute()
        {
            return allowAbsolute;
        }

        public Config allowAbsolute(boolean allowAbsoluteRef)
        {
            allowAbsolute = allowAbsoluteRef;
            return this;
        }

        public boolean allowSectors()
        {
            return allowSectors;
        }

        public Config allowSectors(boolean allowSectorsRef)
        {
            allowSectors = allowSectorsRef;
            return this;
        }

        public boolean allowPercent()
        {
            return allowPercent;
        }

        public Config allowPercent(boolean allowPercentRef)
        {
            allowPercent = allowPercentRef;
            return this;
        }

        public boolean allowNoUnit()
        {
            return allowNoUnit;
        }

        public Config allowNoUnit(boolean allowNoUnitRef)
        {
            allowNoUnit = allowNoUnitRef;
            return this;
        }

        public @Nullable SizeUnit dfltSizeUnit()
        {
            return dfltSizeUnit;
        }

        public Config dfltSizeUnit(@Nullable SizeUnit dfltSizeUnitRef)
        {
            dfltSizeUnit = dfltSizeUnitRef;
            if (dfltSizeUnitRef != null)
            {
                dfltPercent = false;
            }
            return this;
        }

        public boolean dfltPercent()
        {
            return dfltPercent;
        }

        public Config dfltPercent(boolean dfltPercentRef)
        {
            dfltPercent = dfltPercentRef;
            if (dfltPercentRef)
            {
                dfltSizeUnit = null;
                allowPercent = true;
            }
            return this;
        }

        public Config allowRounding(boolean allowRoundingRef)
        {
            allowRounding = allowRoundingRef;
            return this;
        }

        public Config ceil(boolean ceilRef)
        {
            roundCeil = ceilRef;
            return this;
        }
    }

    public static void ensureParsable(String strRef) throws ApiRcException
    {
        ensureParsable(strRef, new Config());
    }

    public static void ensureParsableWithPercent(String strRef)
    {
        ensureParsable(strRef, PARSER_WITH_PERCENT);
    }

    public static void ensureParsable(String strRef, Config cfgRef) throws ApiRcException
    {
        parseImpl(
            strRef,
            cfgRef,
            cause -> new ApiRcException(
                ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_INVLD_CONF, "Failed to parse '" + strRef + "'")
                    .setSkipErrorReport(true),
                cause
            ),
            cause -> new ApiRcException(
                ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_INVLD_CONF, "Failed to parse '" + strRef + "'", cause)
                    .setSkipErrorReport(true)
            )
        );
    }

    public static SizeSpec parse(String strRef) throws LinstorParsingException
    {
        return parse(strRef, new Config());
    }

    public static SizeSpec parse(String strRef, Config cfgRef) throws LinstorParsingException
    {
        return parseImpl(
            strRef,
            cfgRef,
            cause -> new LinstorParsingException("Failed to parse '" + strRef + "'", cause),
            cause -> new LinstorParsingException("Failed to parse '" + strRef + "'", null, cause, null, null)
        );
    }

    /**
     * <p>Parses a given size depending on how the parser is configured.</p>
     * <p>This parser is able to parse strings like "10GiB", "6.5tb" or "10%". The resulting {@link SizeSpec}'s
     * implementation (currently only {@link SizeSpec.Percent} and {@link SizeSpec.Abs}) depend on the suffix of
     * the string or the configure default suffix.</p>
     *
     * @param <EXC> The type of exception the caller wants to receive if the given string cannot be parsed. Mostly
     * introduced to be able to unify {@link #parse(String)} and {@link #ensureParsable(String)}.
     * @param strRef The actual string that should be parsed
     * @param cfgRef The configuration to use for parsing
     * @param throwableExcHandler The exception generator in case a different exception occurred while parsing (Most
     *  likey {@link NumberFormatException})
     * @param strReasonExcHandler The exception generator in case the implemented parser ran into an issue or a
     * violation with the given configuration was detected
     * @return A {@link SizeSpec} implementation representing the parsed size.
     */
    public static <EXC extends Exception> SizeSpec parseImpl(
        String strRef,
        Config cfgRef,
        Function<Throwable, EXC> throwableExcHandler,
        Function<String, EXC> strReasonExcHandler
    )
        throws EXC
    {
        Matcher matcher = PATTERN.matcher(strRef);
        if (!matcher.matches())
        {
            throw strReasonExcHandler.apply("The given string did not match the pattern: " + PATTERN.pattern());
        }

        try
        {
            String numStr = matcher.group(1);
            String unit = matcher.group(2);
            boolean isPercent = unit.contains("%");
            boolean powerOfTwo = forcePowerOfTwo(unit);
            @Nullable SizeUnit sizeUnit;

            if (unit.isBlank())
            {
                sizeUnit = cfgRef.dfltSizeUnit;
                isPercent = cfgRef.dfltPercent;
            }
            else if (!isPercent)
            {
                sizeUnit = SizeUnit.parse(unit, powerOfTwo);
            }
            else
            {
                sizeUnit = null;
            }

            SizeSpec ret;
            if (isPercent)
            {
                ret = new SizeSpec.Percent(Float.parseFloat(numStr));
            }
            else
            {
                ret = parseAbsolute(numStr, sizeUnit, powerOfTwo, cfgRef, strReasonExcHandler);
            }
            runChecks(cfgRef, ret, strReasonExcHandler);
            return ret;
        }
        catch (NumberFormatException nfe)
        {
            throw throwableExcHandler.apply(nfe);
        }
    }

    private static <EXC extends Exception> SizeSpec parseAbsolute(
        String numStrRef,
        @Nullable SizeUnit sizeUnitRef,
        boolean powerOfTwoRef,
        Config cfgRef,
        Function<String, EXC> strReasonExcHandler
    )
        throws EXC
    {
        BigInteger bigInt;
        SizeUnit resolvedSizeUnit;
        if (!numStrRef.contains("."))
        {
            bigInt = new BigInteger(numStrRef);
            resolvedSizeUnit = sizeUnitRef;
        }
        else
        {
            ResolvedSize resolved = resolveFractional(
                numStrRef,
                sizeUnitRef,
                powerOfTwoRef,
                cfgRef,
                strReasonExcHandler
            );
            bigInt = resolved.num;
            resolvedSizeUnit = resolved.unit;
        }

        SizeSpec ret;
        if (bigInt.bitLength() <= LONG_BITS)
        {
            ret = new SizeSpec.Abs(bigInt.longValue(), resolvedSizeUnit);
        }
        else
        {
            throw strReasonExcHandler.apply("Number too large. Maximum " + Long.MAX_VALUE + " supported");
        }
        return ret;
    }

    private record ResolvedSize(BigInteger num, SizeUnit unit)
    {
    }

    private static <EXC extends Exception> ResolvedSize resolveFractional(
        String numStr,
        @Nullable SizeUnit sizeUnit,
        boolean powerOfTwo,
        Config cfgRef,
        Function<String, EXC> strReasonExcHandler
    )
        throws EXC
    {
        String numeratorStr = numStr.replace(".", "");
        int denominatorCount = numStr.length() - (numStr.indexOf(".") + 1);
        BigInteger bigInt = new BigInteger(numeratorStr);
        BigInteger denominatorBigInt = BigInteger.ONE;
        for (int idx = 0; idx < denominatorCount; idx++)
        {
            denominatorBigInt = denominatorBigInt.multiply(BigInteger.TEN);
        }

        HashMap<SizeUnit, SizeUnit> nextLowerMap;
        BigInteger convRate;
        if (powerOfTwo)
        {
            nextLowerMap = NEXT_LOWER_1024;
            convRate = BIG_INT_1024;
        }
        else
        {
            nextLowerMap = NEXT_LOWER_1000;
            convRate = BIG_INT_1000;
        }
        @Nullable SizeUnit nextLowerUnit = nextLowerMap.get(sizeUnit);
        // default remainder 1 to prevent silent accepting of non-rounded numbers
        // for example in case of UNIT_SECTORS
        BigInteger[] divideAndRemainder = new BigInteger[] {
            bigInt, BigInteger.ONE
        };
        SizeUnit resolvedUnit = sizeUnit;
        BigInteger resolvedNum = bigInt;
        while (nextLowerUnit != null)
        {
            bigInt = bigInt.multiply(convRate);
            divideAndRemainder = bigInt.divideAndRemainder(denominatorBigInt);
            if (divideAndRemainder[1].equals(BigInteger.ZERO))
            {
                resolvedNum = divideAndRemainder[0];
                resolvedUnit = nextLowerUnit;
                break;
            }
            nextLowerUnit = nextLowerMap.get(nextLowerUnit);
        }

        if (nextLowerUnit == null && !divideAndRemainder[1].equals(BigInteger.ZERO))
        {
            if (!cfgRef.allowRounding)
            {
                throw strReasonExcHandler.apply(
                    "Input is still not an integer on byte level and rounding is not allowed"
                );
            }
            resolvedNum = divideAndRemainder[0];
            resolvedUnit = SizeUnit.UNIT_B;
            if (cfgRef.roundCeil)
            {
                resolvedNum = resolvedNum.add(BigInteger.ONE);
            }
        }
        return new ResolvedSize(resolvedNum, resolvedUnit);
    }

    private static <EXC extends Exception> void runChecks(
        Config cfgRef,
        SizeSpec specRef,
        Function<String, EXC> strReasonExcHandler
    )
        throws EXC
    {
        if (specRef instanceof SizeSpec.Percent)
        {
            runPercentChecks(cfgRef, strReasonExcHandler);
        }
        else
        {
            runAbsoluteChecks(cfgRef, specRef, strReasonExcHandler);
        }
    }

    private static <EXC extends Exception> void runPercentChecks(
        Config cfgRef,
        Function<String, EXC> strReasonExcHandler
    )
        throws EXC
    {
        if (!cfgRef.allowPercent)
        {
            throw strReasonExcHandler.apply("Parser was configured to not allow percent results");
        }
    }

    @SuppressWarnings("InnerAssignment") // checkstyle cannot deal with "case ... -> ... " syntax
    private static <EXC extends Exception> void runAbsoluteChecks(
        Config cfgRef,
        SizeSpec specRef,
        Function<String, EXC> strReasonExcHandler
    )
        throws EXC
    {
        if (!cfgRef.allowAbsolute)
        {
            throw strReasonExcHandler.apply("Parser was configured to not allow absolute results");
        }
        @Nullable SizeConv.SizeUnit unit;
        switch (specRef)
        {
            case SizeSpec.Abs spec -> unit = spec.unit();
            default -> throw strReasonExcHandler.apply(
                "Unknown / unexpected size: " + specRef + " " + specRef.getClass()
            );
        }
        if (unit == null)
        {
            if (!cfgRef.allowNoUnit)
            {
                throw strReasonExcHandler.apply("Parser was configured to not allow missing units");
            }
        }
        else
        {
            if (!cfgRef.allowSectors && unit.equals(SizeConv.SizeUnit.UNIT_SECTORS))
            {
                throw strReasonExcHandler.apply("Parser was configured to not allow sectors as units");
            }
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static boolean forcePowerOfTwo(String unit)
    {
        return unit.length() == 1 || unit.length() == 3;
    }

    private SizeSpecParser()
    {
    }
}
