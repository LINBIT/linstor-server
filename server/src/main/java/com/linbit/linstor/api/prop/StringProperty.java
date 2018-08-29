package com.linbit.linstor.api.prop;

public class StringProperty implements Property
{
    private String name;
    private String key;
    private boolean internal;
    private String info;

    public StringProperty(String nameRef, String keyRef, boolean internalRef, String infoRef)
    {
        name = nameRef;
        key = keyRef;
        internal = internalRef;
        info = infoRef;
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

}
