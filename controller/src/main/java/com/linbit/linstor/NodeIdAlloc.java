package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.NumberAlloc;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.types.NodeId;

/**
 * Allocator for unoccupied DRBD node id
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class NodeIdAlloc
{
    /**
     * Allocates a free (unused) node id<br/>
     * <br/>
     * This method is the same as calling {@link #getFreeNodeId(int[], NodeId, NodeId)}
     * with {@link NodeId.NODE_ID_MIN} and {@link NodeId.NODE_ID_MAX} as range limitation.
     *
     * @param occupiedIds
     * @return An unoccupied {@link NodeId}
     * @throws ExhaustedPoolException
     */
    public static NodeId getFreeNodeId(int[] occupied) throws ExhaustedPoolException
    {
        NodeId result;
        try
        {
            result = new NodeId(
                NumberAlloc.getFreeNumber(occupied, NodeId.NODE_ID_MIN, NodeId.NODE_ID_MAX)
            );
        }
        catch (ValueOutOfRangeException valueExc)
        {
            throw new ImplementationError(
                "The algorithm allocated an invalid node id",
                valueExc
            );
        }
        return result;
    }

    /**
     * Allocates a free (unused) node id
     *
     * @param occupied List of unique occupied node ids sorted in ascending order
     * @param minNodeId Lower bound of the node id range
     * @param maxNodeId Upper bound of the node id range
     * @return An unoccupied {@link NodeId}
     * @throws ExhaustedPoolException If all node ids within the specified range are occupied
     */
    public static NodeId getFreeNodeId(
        int[] occupied,
        NodeId minNodeId,
        NodeId maxNodeId
    )
        throws ExhaustedPoolException
    {
        NodeId result;
        try
        {
            result = new NodeId(
                NumberAlloc.getFreeNumber(occupied, minNodeId.value, maxNodeId.value)
            );
        }
        catch (ValueOutOfRangeException valueExc)
        {
            throw new ImplementationError(
                "The algorithm allocated an invalid node id",
                valueExc
            );
        }
        return result;
    }

    private NodeIdAlloc()
    {
    }
}
