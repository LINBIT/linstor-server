package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;

import java.util.regex.Pattern;

public class BooleanProperty extends GenericProperty
{
    private static final Pattern PATTERN = Pattern.compile("(?i)(?:yes|no)");

    public BooleanProperty(
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
        return value.toLowerCase();
    }

    @Override
    public String getValue()
    {
        return PATTERN.pattern();
    }

    @Override
    public PropertyType getType()
    {
        return Property.PropertyType.BOOLEAN;
    }

    @Override
    public String getErrorMsg()
    {
        return "This value must be either 'yes' or 'no'.";
    }
}
