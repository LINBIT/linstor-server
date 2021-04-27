package com.linbit.linstor.api.prop;

import java.util.regex.Pattern;

public class RegexProperty extends GenericProperty implements Property
{
    private final Pattern pattern;

    public RegexProperty(
        String nameRef,
        String keyRef,
        String value,
        boolean internalRef,
        String infoRef,
        String unitRef,
        String dfltRef
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
