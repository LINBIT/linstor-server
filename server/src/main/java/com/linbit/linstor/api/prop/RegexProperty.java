package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;

import java.util.regex.Pattern;

public class RegexProperty extends GenericProperty
{
    private final Pattern pattern;

    public RegexProperty(
        String nameRef,
        String keyRef,
        String value,
        boolean internalRef,
        @Nullable String infoRef,
        @Nullable String unitRef,
        @Nullable String dfltRef
    )
    {
        super(nameRef, keyRef, internalRef, infoRef, unitRef, dfltRef);
        pattern = Pattern.compile(value);
    }

    @Override
    public boolean isValid(String value)
    {
        return pattern.matcher(value).matches();
    }

    @Override
    public String getValue()
    {
        return pattern.pattern();
    }

    @Override
    public PropertyType getType()
    {
        return Property.PropertyType.REGEX;
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
