package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.StorageException;

public interface Crypt
{
    String CRYPT_PREFIX = "Linstor-Crypt-";

    String createCryptDevice(String dev, byte[] cryptKey, String identifier)
        throws StorageException;

    void openCryptDevice(String dev, String targetIdentifier, byte[] cryptKey) throws StorageException;

    void closeCryptDevice(String identifier) throws StorageException;

    String getCryptVolumePath(String identifier);

}
