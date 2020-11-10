package com.linbit.linstor.api.prop;

import java.util.regex.Pattern;

public class RegexProperty implements Property
{
    private final String name;
    private final String key;
    private final Pattern pattern;
    private boolean internal;
    private String info;
    private String unit;
    private String dflt;

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
        name = nameRef;
        key = keyRef;
        info = infoRef;
        pattern = Pattern.compile(value);
        internal = internalRef;
        unit = unitRef;
        dflt = dfltRef;
    }

    @Override
    public boolean isValid(String value)
    {
        return pattern.matcher(value).matches();
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
        return pattern.pattern();
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
}
