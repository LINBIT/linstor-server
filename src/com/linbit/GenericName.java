package com.linbit;

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

    public String getName()
    {
        return value;
    }

    public String getDisplayName()
    {
        return displayValue;
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
            if (other instanceof GenericName)
            {
                result = value.equals(((GenericName) other).value);
            }
            else
            if (other instanceof String)
            {
                result = value.equalsIgnoreCase((String) other);
            }
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public String toString()
    {
        return displayValue;
    }


}
