package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;

import java.util.regex.Pattern;

public class NumericOrSymbolProperty extends GenericProperty
{
    private final long min;
    private final long max;
    private final Pattern regex;

    public NumericOrSymbolProperty(
        String nameRef,
        String keyRef,
        long minRef,
        long maxRef,
        String buildValuesEnumRegexRef,
        boolean internalRef,
        @Nullable String infoRef,
        @Nullable String unitRef,
        @Nullable String dfltRef
    )
    {
        super(nameRef, keyRef, internalRef, infoRef, unitRef, dfltRef);
        min = minRef;
        max = maxRef;
        regex = Pattern.compile(buildValuesEnumRegexRef);
    }

    @Override
    public String getValue()
    {
        return "(" + min + " - " + max + ") or " + regex.pattern();
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
    public PropertyType getType()
    {
        return Property.PropertyType.NUMERIC_OR_SYMBOL;
    }

    @Override
    public String getErrorMsg()
    {
        String errorMsg;
        if (super.getUnit() == null)
        {
            errorMsg = "This value has to match " + getValue() + ".";
        }
        else
        {
            errorMsg = "This value  has to match " + getValue() + " " + getUnit() + ".";
        }
        return errorMsg;
    }

    public long getMax()
    {
        return max;
    }

    public long getMin()
    {
        return min;
    }
}
