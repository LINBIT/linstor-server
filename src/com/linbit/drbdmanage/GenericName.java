package com.linbit.drbdmanage;

/**
 * Base class for classes keeping the name of a drbdmanageNG object
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class GenericName implements Comparable<GenericName>
{
    public final String value;
    public final String displayValue;

    public GenericName(String genName)
    {
        value = genName.toUpperCase();
        displayValue = genName;
    }

    @Override
    public int compareTo(GenericName other)
    {
        return value.compareTo(other.value);
    }

    @Override
    public boolean equals(Object other)
    {
        boolean result = false;
        if (this == other)
        {
            result = true;
        }
        else
        {
            result = value.equals(((GenericName) other).value);
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
