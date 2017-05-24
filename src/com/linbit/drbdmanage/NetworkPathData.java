package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import java.util.UUID;

/**
 * Defines a network path between two DRBD resources
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NetworkPathData implements NetworkPath
{
    // Object identifier
    private UUID objId;

    private NetInterface srcInterface;
    private Node         dstNode;
    private NetInterface dstInterface;

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    public NetworkPathData(NetInterface fromInterface, Node toNode, NetInterface toInterface)
    {
        ErrorCheck.ctorNotNull(NetworkPathData.class, NetInterface.class, fromInterface);
        ErrorCheck.ctorNotNull(NetworkPathData.class, Node.class, toNode);
        ErrorCheck.ctorNotNull(NetworkPathData.class, NetInterface.class, toInterface);

        srcInterface = fromInterface;
        dstNode = toNode;
        dstInterface = toInterface;
    }
}
