package com.linbit.linstor;

import com.linbit.ErrorCheck;
import com.linbit.AbsTransactionObject;

import java.util.Arrays;
import java.util.UUID;

/**
 * Defines a network path between two DRBD resources
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
//TODO: gh - should we persist this object too?
public class NetworkPathData extends BaseTransactionObject implements NetworkPath
{
    // Object identifier
    private UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

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

        dbgInstanceId = UUID.randomUUID();

        transObjs = Arrays.asList(
            (AbsTransactionObject) srcInterface,
            (AbsTransactionObject) dstNode,
            (AbsTransactionObject) dstInterface
        );
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }
}
