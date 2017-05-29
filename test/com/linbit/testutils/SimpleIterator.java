package com.linbit.testutils;

import java.util.Arrays;
import java.util.Iterator;

public class SimpleIterator implements Iterator<Object[]>, Iterable<Object[]>
{
    private final int[] currentIdx;
    private final Object[][] values;

    public SimpleIterator(Object[][] values)
    {
        this.values = values;
        currentIdx = new int[values.length];
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
        for (int i = 0; i < currentIdx.length; ++i)
        {
            if (currentIdx[i] < values[i].length - 1)
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
        for (int i = 0; i < currentIdx.length; ++i)
        {
            next[i] = values[i][currentIdx[i]];
        }
        return next;
    }

    private void incrementIdx()
    {
        for (int i = 0; i < currentIdx.length; ++i)
        {
            if (++currentIdx[i] >= values[i].length)
            {
                if (i != values.length - 1)
                {
                    currentIdx[i] = 0;
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
