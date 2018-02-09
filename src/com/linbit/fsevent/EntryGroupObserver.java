package com.linbit.fsevent;

/**
 * FileSystemWatch interface for observing changes on a group of file paths
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface EntryGroupObserver
{
    /**
     * Called when the event specified for each file path in the group has occurred
     *
     * @param group The FileEntryGroup that specifies the file paths and events
     *     that have been waited for
     */
    void entryGroupEvent(FileSystemWatch.FileEntryGroup group);
}
