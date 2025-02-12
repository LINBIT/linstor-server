package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;

public class RangeProperty extends GenericProperty
{
    private final long min;
    private final long max;

    public RangeProperty(
        String nameRef,
        String keyRef,
        long minRef,
        long maxRef,
        boolean internalRef,
        @Nullable String infoRef,
        @Nullable String unitRef,
        @Nullable String dfltRef
    )
    {
        super(nameRef, keyRef, internalRef, infoRef, unitRef, dfltRef);
        min = minRef;
        max = maxRef;
    }

    public long getMin()
    {
        return min;
    }

    public long getMax()
    {
        return max;
    }

    @Override
    public String getValue()
    {
        return "(" + min + " - " + max + ")";
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
        catch (NumberFormatException ignored)
        {
        }
        return valid;
    }

    @Override
    public PropertyType getType()
    {
        return Property.PropertyType.RANGE;
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

}
