package com.linbit.linstor.api.interfaces.serializer;

import java.util.Collection;

import com.linbit.linstor.Node;

public interface CtrlNodeSerializer
{
    public byte[] getChangedMessage(Node satelliteNode);

    public byte[] getDataMessage(int msgId, Node satelliteNode, Collection<Node> otherNodes);
}
