package com.linbit.drbdmanage.dbdrivers;

import java.util.Arrays;

public class PrimaryKey
{
    public Object[] keys;

    public PrimaryKey(Object... keyRefs)
    {
        keys = keyRefs;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(keys);
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PrimaryKey other = (PrimaryKey) obj;
        if (!Arrays.equals(keys, other.keys))
            return false;
        return true;
    }
}
