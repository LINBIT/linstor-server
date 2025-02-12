package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;

import java.util.regex.Pattern;

public class BooleanTrueFalseProperty extends GenericProperty
{
    private static final Pattern PATTERN = Pattern.compile("(?i)(?:true|false|yes|no)");

    public BooleanTrueFalseProperty(
        String nameRef,
        String keyRef,
        boolean internalRef,
        @Nullable String infoRef,
        @Nullable String unitRef,
        @Nullable String dfltRef
    )
    {
        super(nameRef, keyRef, internalRef, infoRef, unitRef, dfltRef);
    }

    @Override
    public boolean isValid(String value)
    {
        return PATTERN.matcher(value).matches();
    }

    @Override
    public String normalize(String value)
    {
        return Boolean.toString(
            value.equalsIgnoreCase("true") ||
                value.equalsIgnoreCase("yes")
        );
    }

    @Override
    public String getValue()
    {
        return PATTERN.pattern();
    }

    @Override
    public PropertyType getType()
    {
        return Property.PropertyType.BOOLEAN_TRUE_FALSE;
    }

    @Override
    public String getErrorMsg()
    {
        return "This value must be either 'yes', 'no', 'true' or 'false'.";
    }
}
