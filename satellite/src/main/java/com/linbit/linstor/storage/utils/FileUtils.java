package com.linbit.linstor.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class FileUtils
{
    private FileUtils()
    {
    }

    public static class FileInfo
    {
        public final Path directory;
        public final String identifier;
        public final Path loPath;
        public final long size;

        FileInfo(Path loPathRef, Path backingPathRef)
        {
            loPath = loPathRef;

            directory = backingPathRef.getParent();
            identifier = backingPathRef.getFileName().toString();

            size = backingPathRef.toFile().length();
        }
    }

    public static Map<String, FileInfo> getInfoList(ExtCmd extCmd)
        throws StorageException
    {
        OutputData outputData = LosetupCommands.list(extCmd);

        final Map<String, FileInfo> ret = new HashMap<>();
        final String stdOut = new String(outputData.stdoutData);
        if (!stdOut.trim().isEmpty())
        {
            for (final String line : stdOut.split("\n"))
            {
                final String[] data = line.trim().split("\\s+");
                if (!data[LosetupCommands.LOSETUP_LIST_BACK_FILE_IDX].equals("BACK_FILE"))
                {
                    ret.put(
                        data[LosetupCommands.LOSETUP_LIST_BACK_FILE_IDX],
                        new FileInfo(
                            Paths.get(data[LosetupCommands.LOSETUP_LIST_DEV_NAME_IDX]),
                            Paths.get(data[LosetupCommands.LOSETUP_LIST_BACK_FILE_IDX])
                            )
                        );
                }
            }
        }
        return ret;
    }


    public static Map<String, Long> getDirFreeSizes(Set<String> changedStoragePoolStringsRef)
    {
        Map<String, Long> ret = new TreeMap<>();
        for (String dir : changedStoragePoolStringsRef)
        {
            File file = Paths.get(dir).toFile();
            if (file.exists())
            {
                ret.put(
                    dir,
                    file.getFreeSpace()
                );
            }
        }
        return ret;
    }
}
