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

    public AbsIterator(Object[][] valuesRef, int[] skipColumns)
    {
        this.values = valuesRef;
        currentIdx = new int[valuesRef.length];
        usedColumns = new int[valuesRef.length - skipColumns.length];
        int usedIdx = 0;
        int skipIdx = 0;
        for (int valueIdx = 0; valueIdx < valuesRef.length; ++valueIdx)
        {
            if (skipIdx < skipColumns.length && valueIdx == skipColumns[skipIdx])
            {
                ++skipIdx;
            }
            else
            {
                usedColumns[usedIdx] = valueIdx;
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
        for (int usedCol : usedColumns)
        {
            if (currentIdx[usedCol] < values[usedCol].length - 1)
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
        catch (Exception exc)
        {
            throw new RuntimeException(exc);
        }
        return currentIteration;
    }

    protected abstract T getNext() throws Exception;

    private void incrementIdx()
    {
        for (int usedCol : usedColumns)
        {
            ++currentIdx[usedCol];
            if (currentIdx[usedCol] >= values[usedCol].length)
            {
                if (usedCol != values.length - 1)
                {
                    currentIdx[usedCol] = 0;
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
        for (int idx = 0; idx < currentIdx.length; ++idx)
        {
            currentIdx[idx] = 0;
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
