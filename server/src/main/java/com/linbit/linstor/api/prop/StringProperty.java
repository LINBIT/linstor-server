package com.linbit.linstor.api.prop;

public class StringProperty implements Property
{
    private String name;
    private String key;
    private boolean internal;
    private String info;
    private String unit;
    private String dflt;

    public StringProperty(
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

}
