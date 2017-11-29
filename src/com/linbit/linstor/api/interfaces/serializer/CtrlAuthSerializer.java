package com.linbit.linstor.api.interfaces.serializer;

import java.io.IOException;

public interface CtrlAuthSerializer
{
    byte[] getAuthMessage(byte[] sharedSecret) throws IOException;
}
