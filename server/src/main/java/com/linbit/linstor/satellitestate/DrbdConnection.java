package com.linbit.linstor.satellitestate;

import com.linbit.linstor.core.identifier.NodeName;

public class DrbdConnection
{
    private final NodeName nodeA;
    private final NodeName nodeB;

    public DrbdConnection(NodeName node1, NodeName node2)
    {
        if (node1.getName().compareTo(node2.getName()) < 0)
        {
            nodeA = node1;
            nodeB = node2;
        }
        else
        {
            nodeA = node2;
            nodeB = node1;
        }
    }

    @Override
    public int hashCode()
    {
        return nodeA.hashCode() & nodeB.hashCode();
    }

    @Override
    public boolean equals(Object oth)
    {
        if (oth instanceof DrbdConnection)
        {
            DrbdConnection other = (DrbdConnection) oth;
            return nodeA.equals(other.nodeA) && nodeB.equals(other.nodeB);
        }
        return false;
    }
}
