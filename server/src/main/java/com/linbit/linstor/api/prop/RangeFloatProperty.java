package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;

public class RangeFloatProperty extends GenericProperty
{
    private final double min;
    private final double max;

    public RangeFloatProperty(
        String nameRef,
        String keyRef,
        double minRef,
        double maxRef,
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

    public double getMin()
    {
        return min;
    }

    public double getMax()
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
            double val = Double.parseDouble(value);
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
        return Property.PropertyType.RANGE_FLOAT;
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
