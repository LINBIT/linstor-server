package com.linbit.linstor.core.types;

import com.linbit.Checks;
import com.linbit.ValueOutOfRangeException;

public class NodeId implements Comparable<NodeId>
{
    public static final int NODE_ID_MIN = 0;
    public static final int NODE_ID_MAX = 31;

    private static final String NODE_ID_EXC_FORMAT =
        "Node ID %d is out of range [%d - %d]";

    public final int value;

    public NodeId(final int idValue) throws ValueOutOfRangeException
    {
        nodeIdCheck(idValue);
        value = idValue;
    }

    @Override
    public int compareTo(NodeId other)
    {
        int result;
        if (other == null)
        {
            // null sorts before any existing node id
            result = 1;
        }
        else
        {
            result = Integer.compare(value, other.value);
        }
        return result;
    }

    @Override
    public boolean equals(Object other)
    {
        return other != null &&
            (other instanceof NodeId) &&
            ((NodeId) other).value == this.value;
    }

    @Override
    public int hashCode()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return Integer.toString(value);
    }

    /**
     * Checks the validity of a DRBD node id
     *
     * @param value The node id to check
     * @throws ValueOutOfRangeException If the node id is out of range
     */
    public static void nodeIdCheck(int value) throws ValueOutOfRangeException
    {
        Checks.genericRangeCheck(value, NODE_ID_MIN, NODE_ID_MAX, NODE_ID_EXC_FORMAT);
    }
}
