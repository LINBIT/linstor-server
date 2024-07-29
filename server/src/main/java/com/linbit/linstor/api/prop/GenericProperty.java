package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;

public class GenericProperty implements Property
{
    private final String name;
    private final String key;
    private final boolean internal;
    private final String info;
    private final String unit;
    private final String dflt;

    public GenericProperty(
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
        internal = internalRef;
        info = infoRef;
        unit = unitRef;
        dflt = dfltRef;
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
        return "--no restrictions--";
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
    public boolean isValid(String value)
    {
        return true;
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
    public @Nullable PropertyType getType()
    {
        return null;
    }

    @Override
    public String getErrorMsg()
    {
        return "This value has no restrictions.";
    }

}
