package com.linbit.linstor.api.interfaces;

import java.io.IOException;

public interface AuthSerializer
{
    byte[] getAuthMessage(byte[] sharedSecret) throws IOException;
}
