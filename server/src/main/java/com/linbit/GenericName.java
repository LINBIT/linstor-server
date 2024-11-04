package com.linbit;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Base class for classes keeping the name of a linstor object
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class GenericName implements Comparable<GenericName>
{
    public final String value;
    public final String displayValue;

    public GenericName(String genName) throws InvalidNameException
    {
        if (genName == null)
        {
            throw new InvalidNameException("Given name is null", "null");
        }
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

    @SuppressFBWarnings("EQ_CHECK_FOR_OPERAND_NOT_COMPATIBLE_WITH_THIS")
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

    /**
     * sorts in this order:
     * "a", "b", null
     */
    public static <NAME extends GenericName> int compareToNullable(NAME nameRef, NAME name2Ref)
    {
        int cmp;
        if (nameRef != null)
        {
            if (name2Ref != null)
            {
                cmp = nameRef.compareTo(name2Ref);
            }
            else
            {
                cmp = -1;
            }
        }
        else
        {
            if (name2Ref != null)
            {
                cmp = 1;
            }
            else
            {
                cmp = 0;
            }
        }
        return cmp;
    }
}
