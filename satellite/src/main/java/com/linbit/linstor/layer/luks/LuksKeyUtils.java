package com.linbit.linstor.layer.luks;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;

public final class LuksKeyUtils
{
    private LuksKeyUtils()
    {
    }

    /**
     * Decrypts the LUKS key of the given resource or snapshot volume data with the master key and stores it as the
     * decrypted password of the volume data.
     *
     * @return the decrypted key
     */
    public static byte[] decryptKey(DecryptionHelper decryptionHelper, byte[] masterKey, LuksVlmData<?> vlmData)
        throws LinStorException
    {
        byte[] decryptedKey = decryptionHelper.decrypt(masterKey, vlmData.getEncryptedKey());
        vlmData.setDecryptedPassword(decryptedKey);
        return decryptedKey;
    }
}
