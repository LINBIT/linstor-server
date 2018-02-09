package com.linbit.fsevent;

/**
 * FileSystemWatch interface for observing changes on a file path
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface FileObserver
{
    /**
     * Called when the specified event occurred on a file that is being watched for changes
     *
     * @param watchEntry The entry that specifies the file path to watch
     */
    void fileEvent(FileSystemWatch.FileEntry watchEntry);
}
