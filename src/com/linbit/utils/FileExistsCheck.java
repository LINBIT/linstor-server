package com.linbit.utils;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Checks whether a file exists
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class FileExistsCheck implements Runnable
{
    public static final long DFLT_CHECK_TIMEOUT = 15_000L;
    
    private final String filePath;
    private boolean existsFlag;

    public FileExistsCheck(String path, boolean existsDefault)
    {
        filePath = path;
        existsFlag = existsDefault;
    }

    @Override
    public void run()
    {
        Path chkPath = FileSystems.getDefault().getPath(filePath);
        existsFlag = Files.exists(chkPath);
    }

    public boolean fileExists()
    {
        return existsFlag;
    }
}
