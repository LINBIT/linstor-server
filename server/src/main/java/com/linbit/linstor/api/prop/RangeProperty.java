package com.linbit.linstor.api.prop;

public class RangeProperty implements Property
{
    private String name;
    private String key;
    private long min;
    private long max;
    private boolean internal;
    private String info;
    private String unit;
    private String dflt;

    public RangeProperty(
        String nameRef,
        String keyRef,
        long minRef,
        long maxRef,
        boolean internalRef,
        String infoRef,
        String unitRef,
        String dfltRef
    )
    {
        name = nameRef;
        key = keyRef;
        min = minRef;
        max = maxRef;
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
        return "(" + min + "-" + max + ")";
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
        boolean valid = false;
        try
        {
            long val = Long.parseLong(value);
            valid = min <= val && val <= max;
        }
        catch (NumberFormatException ignored)
        {
        }
        return valid;
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
