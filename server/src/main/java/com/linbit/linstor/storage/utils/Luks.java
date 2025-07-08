package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.StorageException;
import java.util.List;

public interface Luks
{
    String createLuksDevice(
        String dev,
        byte[] cryptKey,
        String identifier,
        List<String> additionalOptions
    ) throws StorageException;

    void openLuksDevice(
        String dev,
        String targetIdentifier,
        byte[] cryptKey,
        boolean readOnly,
        List<String> additionalOptions
    ) throws StorageException;

    void closeLuksDevice(String identifier) throws StorageException;

    String getLuksVolumePath(String identifier);

}
