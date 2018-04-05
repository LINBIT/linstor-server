package com.linbit.testutils;

import java.util.Arrays;
import java.util.Iterator;

import com.linbit.linstor.security.SecurityLevel;

public abstract class AbsIterator<T> implements Iterator<T>, Iterable<T>
{
    private final Object[][] values;
    private final int[] currentIdx;
    private final int[] usedColumns;
    public T currentIteration;

    public AbsIterator(Object[][] values, int[] skipColumns)
    {
        this.values = values;
        currentIdx = new int[values.length];
        usedColumns = new int[values.length - skipColumns.length];
        int usedIdx = 0;
        int skipIdx = 0;
        for (int i = 0; i < values.length; ++i)
        {
            if (skipIdx < skipColumns.length && i == skipColumns[skipIdx])
            {
                ++skipIdx;
            }
            else
            {
                usedColumns[usedIdx] = i;
                ++usedIdx;
            }
        }
        resetAllIdx();
    }

    @Override
    public boolean hasNext()
    {
        return hasNextCombination();
    }

    public boolean hasNextCombination()
    {
        boolean hasNext = false;
        for (int i : usedColumns)
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
    public T next()
    {
        incrementIdx();
        try
        {
            currentIteration = getNext();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return currentIteration;
    }

    protected abstract T getNext() throws Exception;

    private void incrementIdx()
    {
        for (int i : usedColumns)
        {
            ++currentIdx[i];
            if (currentIdx[i] >= values[i].length)
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

    protected void resetAllIdx()
    {
        for (int i = 0; i < currentIdx.length; ++i)
        {
            currentIdx[i] = 0;
        }
        currentIdx[usedColumns[0]] = -1; // we increment it at the start of next()
    }

    @SuppressWarnings("unchecked")
    protected <V> V getValue(int column)
    {
        return (V) values[column][currentIdx[column]];
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(SecurityLevel.get());
        sb.append(" ");
        sb.append(Arrays.toString(currentIdx));
        return sb.toString();
    }

    @Override
    public Iterator<T> iterator()
    {
        return this;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("Remove is not supported by this iterator");
    }
}
