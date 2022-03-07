package com.linbit.crypto;

import com.linbit.linstor.LinStorException;

public interface KeyDerivation
{
    byte[] passphraseToKey(
        byte[] passphrase,
        byte[] salt
    )
        throws LinStorException;
}
