package com.linbit.crypto;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;

public interface KeyDerivation
{
    @Nullable
    byte[] passphraseToKey(
        byte[] passphrase,
        byte[] salt
    )
        throws LinStorException;
}
