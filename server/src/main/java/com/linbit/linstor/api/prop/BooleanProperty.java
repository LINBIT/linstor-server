package com.linbit.linstor.api.prop;

import java.util.regex.Pattern;

public class BooleanProperty implements Property
{
    private static final Pattern PATTERN = Pattern.compile("(?i)(?:yes|no)");

    private final String name;
    private final String key;
    private boolean internal;
    private String info;
    private String unit;
    private String dflt;

    public BooleanProperty(
        String nameRef,
        String keyRef,
        boolean internalRef,
        String infoRef,
        String unitRef,
        String dfltRef
    )
    {
        name = nameRef;
        key = keyRef;
        info = infoRef;
        internal = internalRef;
        unit = unitRef;
        dflt = dfltRef;
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

    @Override
    public PropertyType getType()
    {
        return Property.PropertyType.BOOLEAN;
    }
}
