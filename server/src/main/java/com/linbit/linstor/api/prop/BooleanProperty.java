package com.linbit.linstor.api.prop;

import java.util.regex.Pattern;

public class BooleanProperty implements Property
{
    private static final Pattern PATTERN = Pattern.compile("(?i)(?:true|false|yes|no)");

    private final String name;
    private final String key;
    private boolean internal;
    private String info;

    public BooleanProperty(
        String nameRef,
        String keyRef,
        boolean internalRef,
        String infoRef
    )
    {
        name = nameRef;
        key = keyRef;
        info = infoRef;
        internal = internalRef;
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
        return PATTERN.pattern();
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
}
