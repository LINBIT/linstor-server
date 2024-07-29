package com.linbit.linstor.modularcrypto;

public enum CipherStrength
{
    KEY_LENGTH_128(128),
    KEY_LENGTH_192(192),
    KEY_LENGTH_256(256);

    public final int keyLength;

    CipherStrength(int length)
    {
        keyLength = length;
    }
}
