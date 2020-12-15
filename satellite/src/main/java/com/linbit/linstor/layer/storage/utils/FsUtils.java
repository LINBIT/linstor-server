package com.linbit.linstor.layer.storage.utils;

import com.linbit.linstor.storage.StorageException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FsUtils
{
    public static String readAllBytes(Path path) throws StorageException
    {
        try
        {
            return new String(Files.readAllBytes(path));
        }
        catch (IOException ioExc)
        {
            throw new StorageException("An IOException occurred while reading '" + path + "'.", ioExc);
        }
    }
}
