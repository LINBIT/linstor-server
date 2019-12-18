package com.linbit.linstor.storage.utils;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
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

        public FileInfo(Path loPathRef, Path backingPathRef)
        {
            loPath = loPathRef;

            directory = backingPathRef.getParent();
            identifier = backingPathRef.getFileName().toString();

            size = SizeConv.convert(
                backingPathRef.toFile().length(),
                SizeUnit.UNIT_B,
                SizeUnit.UNIT_KiB
            );
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
            final String[] lines = stdOut.split("\n");
            // idx starts at 1 so we can skip the HEADER row "NAME BACK-FILE"
            for (int idx = 1; idx < lines.length; ++idx)
            {
                final String line = lines[idx];
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


    public static long getPoolCapacity(ExtCmd extCmd, Path storageDirectoryRef) throws StorageException
    {
        return parseSimpleDfOutputAsLong(FileCommands.getTotalCapacity(extCmd, storageDirectoryRef));
    }

    public static long getFreeSpace(ExtCmd extCmd, Path storageDirectoryRef) throws StorageException
    {
        return parseSimpleDfOutputAsLong(FileCommands.getFreeSpace(extCmd, storageDirectoryRef));
    }

    public static String getSourceDevice(ExtCmd extCmd, Path storageDirectoryRef) throws StorageException
    {
        return parseSimpleDfOutputAsString(FileCommands.getSourceDevice(extCmd, storageDirectoryRef));
    }

    private static long parseSimpleDfOutputAsLong(OutputData outputData)
    {
        String sizeStr = parseSimpleDfOutputAsString(outputData);
        return SizeConv.convert(
            Long.parseLong(sizeStr),
            SizeUnit.UNIT_B,
            SizeUnit.UNIT_KiB
        );
    }

    private static String parseSimpleDfOutputAsString(OutputData outputData)
    {
        String outStr = new String(outputData.stdoutData);
        String data = outStr.split("\n")[1]; // [0] is the header
        return data.trim();
    }
}
