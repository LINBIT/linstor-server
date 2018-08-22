package com.linbit.linstor.storage.utils;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;

public interface Crypt
{
    @FunctionalInterface
    interface OutputDataVerifier
    {
        void verifyOutput(OutputData outData, String[] command)
            throws StorageException;
    }

    String CRYPT_PREFIX = "Linstor-Crypt-";

    String createCryptDevice(String dev, byte[] cryptKey, OutputDataVerifier outVerifier, String identifier)
        throws StorageException;

    void openCryptDevice(String dev, String targetIdentifier, byte[] cryptKey) throws StorageException;

    void closeCryptDevice(String identifier) throws StorageException;

    String getCryptVolumePath(String identifier);

}
