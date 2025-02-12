package com.linbit.linstor.api.prop;

import com.linbit.linstor.annotation.Nullable;

public abstract class GenericProperty implements Property
{
    private final String name;
    private final String key;
    private final boolean internal;
    private final @Nullable String info;
    private final @Nullable String unit;
    private final @Nullable String dflt;

    public GenericProperty(
        String nameRef,
        String keyRef,
        boolean internalRef,
        @Nullable String infoRef,
        @Nullable String unitRef,
        @Nullable String dfltRef
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
    public @Nullable String getInfo()
    {
        return info;
    }

    @Override
    public boolean isValid(String value)
    {
        return true;
    }

    @Override
    public @Nullable String getUnit()
    {
        return unit;
    }

    @Override
    public @Nullable String getDflt()
    {
        return dflt;
    }

    @Override
    public String getErrorMsg()
    {
        return "This value has no restrictions.";
    }

}
