package com.linbit.linstor.core.objects;

public class SecConfiguration
{
    private String displayValue;
    private String value;

    public SecConfiguration(String displayValue, String value)
    {
        this.displayValue = displayValue;
        this.value = value;
    }

    public String getDisplayValue()
    {
        return displayValue;
    }

    public void setDisplayValue(String displayValue)
    {
        this.displayValue = displayValue;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }
}
