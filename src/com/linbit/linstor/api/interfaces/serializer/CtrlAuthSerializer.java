package com.linbit.linstor.api.interfaces.serializer;

import java.io.IOException;

import com.linbit.linstor.Node;

public interface CtrlAuthSerializer
{
    byte[] getAuthMessage(Node node, byte[] bytes) throws IOException;
}
