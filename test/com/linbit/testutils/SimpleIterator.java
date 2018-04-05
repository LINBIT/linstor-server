package com.linbit.testutils;

import java.util.Arrays;
import java.util.Iterator;

public class SimpleIterator implements Iterator<Object[]>, Iterable<Object[]>
{
    private final int[] currentIdx;
    private final Object[][] values;

    public SimpleIterator(Object[][] valuesRef)
    {
        values = valuesRef;
        currentIdx = new int[valuesRef.length];
        Arrays.fill(currentIdx, 0);
        currentIdx[0] = -1; // next() first triggers an increment
    }

    @Override
    public Iterator<Object[]> iterator()
    {
        return this;
    }

    @Override
    public boolean hasNext()
    {
        boolean hasNext = false;
        for (int idx = 0; idx < currentIdx.length; ++idx)
        {
            if (currentIdx[idx] < values[idx].length - 1)
            {
                hasNext = true;
                break;
            }
        }
        return hasNext;
    }

    @Override
    public Object[] next()
    {
        incrementIdx();
        Object[] next = new Object[currentIdx.length];
        for (int idx = 0; idx < currentIdx.length; ++idx)
        {
            next[idx] = values[idx][currentIdx[idx]];
        }
        return next;
    }

    private void incrementIdx()
    {
        for (int idx = 0; idx < currentIdx.length; ++idx)
        {
            if (++currentIdx[idx] >= values[idx].length)
            {
                if (idx != values.length - 1)
                {
                    currentIdx[idx] = 0;
                }
            }
            else
            {
                break;
            }
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("This iterator does not support removing elements");
    }

    @Override
    public String toString()
    {
        return Arrays.toString(currentIdx);
    }
}
