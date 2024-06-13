package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.StorageException;

public interface Luks
{
    String createLuksDevice(String dev, byte[] cryptKey, String identifier)
        throws StorageException;

    void openLuksDevice(String dev, String targetIdentifier, byte[] cryptKey, boolean readOnly) throws StorageException;

    void closeLuksDevice(String identifier) throws StorageException;

    String getLuksVolumePath(String identifier);

}
