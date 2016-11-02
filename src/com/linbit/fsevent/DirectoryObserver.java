package com.linbit.fsevent;

import java.nio.file.Path;

/**
 * FileSystemWatch interface for observing changes in a filesystem directory
 *
 * @author Robert Altnoeder <robert.altnoeder@linbit.com>
 */
public interface DirectoryObserver
{
    /**
     * Called when an event occurs on the directory specified by watchEntry
     *
     * @param watchEntry The entry that specifies the directory that is being watched
     * @param filePath Path of the file that triggered the event
     */
    public void directoryEvent(FileSystemWatch.DirectoryEntry watchEntry, Path filePath);
}
