package com.linbit.linstor.api.prop;

import java.util.regex.Pattern;

public class NumericOrSymbolProperty implements Property
{
    private final String name;
    private final String key;
    private final long min;
    private final long max;
    private final Pattern regex;
    private final boolean internal;
    private final String info;
    private String unit;
    private String dflt;

    public NumericOrSymbolProperty(
        String nameRef,
        String keyRef,
        long minRef,
        long maxRef,
        String buildValuesEnumRegexRef,
        boolean internalRef,
        String infoRef,
        String unitRef,
        String dfltRef
    )
    {
        name = nameRef;
        key = keyRef;
        min = minRef;
        max = maxRef;
        regex = Pattern.compile(buildValuesEnumRegexRef);
        internal = internalRef;
        info = infoRef;
        unit = unitRef;
        dflt = dfltRef;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getKey()
    {
        return key;
    }

    @Override
    public String getValue()
    {
        return "(" + min + "-" + max + ") or " + regex.pattern();
    }

    @Override
    public boolean isInternal()
    {
        return internal;
    }

    @Override
    public String getInfo()
    {
        return info;
    }

    @Override
    public boolean isValid(String value)
    {
        boolean valid = false;
        try
        {
            long val = Long.parseLong(value);
            valid = min <= val && val <= max;
        }
        catch (NumberFormatException nfexc)
        {
            valid = regex.matcher(value).matches();
        }
        return valid;
    }

    @Override
    public String getUnit()
    {
        return unit;
    }

    @Override
    public String getDflt()
    {
        return dflt;
    }

}
