package com.linbit.fsevent;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.NegativeTimeException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Watches file system paths for changes
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class FileSystemWatch implements Runnable, SystemService
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "Filesystem event tracking service";

    private ServiceName serviceInstanceName;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("FileEventService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                String.format(
                    "%s class contains an invalid name constant",
                    FileSystemWatch.class.getName()
                ),
                nameExc
            );
        }
    }

    public enum Event
    {
        CREATE,
        DELETE,
        CHANGE
    }

    // Synchronization lock for all map access (watchMap, dirMap, fileMap)
    private final Object mapLock;

    // Global event id counter for nextEventId()
    private static long globalEventId = 0;

    private FileSystem fileSys;
    private WatchService wSvc;

    // The thread that handles events on watched directories, if it is active,
    // otherwise null
    private @Nullable Thread watchThread;

    // Flag for shutting down the active watchThread
    private final AtomicBoolean stopFlag;

    // Map from watched directory path to the associated WatchKey object and
    // all active watchers (FileEntry and DirectoryEntry objects)
    private final Map<Path, WatchMapEntry> watchMap;

    // Map from directory path to Entry object
    //
    // This map enables fast lookup of DirectoryEntry objects by path
    private final Map<Path, Set<DirectoryEntry>> dirMap;

    // Map from file path to Entry object
    //
    // This map enables fast lookup of FileEntry objects by path
    private final Map<Path, Set<FileEntry>> fileMap;

    private final ErrorReporter errorReporter;

    @Inject
    public FileSystemWatch(ErrorReporter errorReporterRef) throws IOException
    {
        errorReporter = errorReporterRef;
        serviceInstanceName = SERVICE_NAME;

        mapLock = new Object();

        fileSys = FileSystems.getDefault();
        wSvc = fileSys.newWatchService();
        stopFlag = new AtomicBoolean();
        watchThread = null;

        watchMap = new TreeMap<>();
        dirMap = new TreeMap<>();
        fileMap = new TreeMap<>();
    }

    /**
     * Event loop
     */
    @Override
    public void run()
    {
        List<FileEntry> fileObs = new LinkedList<>();
        List<DirectoryEntry> dirObs = new LinkedList<>();
        while (!stopFlag.get())
        {
            WatchKey polledKey = null;
            try
            {
                // Using poll with a timeout because it is interruptible,
                // whereas the timeout-less poll() does not seem to be interruptible
                polledKey = wSvc.poll(1, TimeUnit.DAYS);
            }
            catch (InterruptedException ignored)
            {
            }
            if (polledKey != null)
            {
                for (WatchEvent<?> event : polledKey.pollEvents())
                {
                    Kind<?> eventKind = event.kind();
                    if (eventKind == StandardWatchEventKinds.OVERFLOW)
                    {
                        // TODO: Read all directories and compare content against
                        //       the files that are being watched
                        // TODO: Notify all directory observers with a placeholder
                        //       DirectoryEntry object to inform those about the
                        //       overflow
                    }
                    else
                    if (eventKind == StandardWatchEventKinds.ENTRY_CREATE ||
                        eventKind == StandardWatchEventKinds.ENTRY_DELETE ||
                        eventKind == StandardWatchEventKinds.ENTRY_MODIFY)
                    {
                        WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                        Path watchedPath = (Path) polledKey.watchable();
                        Path relativePath = pathEvent.context();
                        Path filePath = watchedPath.resolve(relativePath);

                        synchronized (mapLock)
                        {
                            // Collect all file observers
                            Set<FileEntry> fileEntryList = fileMap.get(filePath);
                            if (fileEntryList != null)
                            {
                                Iterator<FileEntry> fileEntryIter = fileEntryList.iterator();
                                while (fileEntryIter.hasNext())
                                {
                                    FileEntry watchEntry = fileEntryIter.next();
                                    if ((watchEntry.watchEvent == Event.CREATE &&
                                        eventKind == StandardWatchEventKinds.ENTRY_CREATE) ||
                                        (watchEntry.watchEvent == Event.DELETE &&
                                        eventKind == StandardWatchEventKinds.ENTRY_DELETE) ||
                                        (watchEntry.watchEvent == Event.CHANGE &&
                                        eventKind == StandardWatchEventKinds.ENTRY_MODIFY))
                                    {
                                        fileObs.add(watchEntry);
                                        if (watchEntry.remove)
                                        {
                                            fileEntryIter.remove();
                                            removeWatcher(watchedPath, watchEntry);
                                        }
                                    }
                                }
                            }

                            // Collect all directory observers
                            Set<DirectoryEntry> dirEntryList = dirMap.get(watchedPath);
                            if (dirEntryList != null)
                            {
                                for (DirectoryEntry watchEntry : dirEntryList)
                                {
                                    if ((watchEntry.watchEvent == Event.CREATE &&
                                        eventKind == StandardWatchEventKinds.ENTRY_CREATE) ||
                                        (watchEntry.watchEvent == Event.DELETE &&
                                        eventKind == StandardWatchEventKinds.ENTRY_DELETE) ||
                                        (watchEntry.watchEvent == Event.CHANGE &&
                                        eventKind == StandardWatchEventKinds.ENTRY_MODIFY))
                                    {
                                        dirObs.add(watchEntry);
                                    }
                                }
                            }
                        }
                        // Trigger all file observers
                        for (FileEntry watchEntry : fileObs)
                        {
                            watchEntry.fObserver.fileEvent(watchEntry);
                        }
                        fileObs.clear();
                        // Trigger all directory observers
                        for (DirectoryEntry watchEntry : dirObs)
                        {
                            watchEntry.dObserver.directoryEvent(watchEntry, filePath);
                        }
                        dirObs.clear();
                    }
                }
                polledKey.reset();
            }
        }
        synchronized (this)
        {
            if (watchThread == Thread.currentThread())
            {
                watchThread = null;
            }
        }
    }

    @Override
    public synchronized void start()
    {
        if (watchThread == null)
        {
            stopFlag.set(false);
            watchThread = new Thread(this);
            watchThread.setName(serviceInstanceName.getDisplayName());
            watchThread.start();
        }
    }

    /**
     * Shuts down this instance's event loop
     *
     * This method does not wait until the event loop has stopped
     */
    @Override
    public synchronized void shutdown()
    {
        stopFlag.set(true);
        if (watchThread != null)
        {
            watchThread.interrupt();
        }
    }

    /**
     * @throws IOException
     *
     */
    void cancelAllWatchKeys() throws IOException
    {
        synchronized (mapLock)
        {
            Collection<WatchMapEntry> entries = watchMap.values();
            for (WatchMapEntry entry : entries)
            {
                if (entry != null)
                {
                    entry.key.cancel();
                }
            }
            wSvc.close();
        }
    }

    /**
     * Adds a new entry that watches for the specified event to happen to the specified file
     *
     * The observer will be called upon occurrence of the specified event on the
     * specified file.
     *
     * Note that the observer callback may take place before the newFileEntry() method returns.
     * Depending on when a file event is detected, the callback may be performed in the
     * context of the thread that called the newFileEntry() method or asynchronously in
     * the context of the FileSystemWatch instance's event loop thread.
     *
     * In either case, no locks are held while the callback to the observer's
     * fileEvent() method takes place.
     *
     * File creation is detected
     *   - if the file exists already when the newFileEntry() method is called
     *   - if the file is created after the newFileEntry() method returned
     *   - if the file's creation races with a call of the newFileEntry() method
     *
     * Entries added by this method are always auto-removed when the event they are
     * watching happens. The observer is only called once.
     *
     * If the file's parent directory can not be watched because it does not exist or is
     * inaccessible, or if the file system fails to perform the necessary actions to install
     * the necessary notification services, an IOException is thrown.
     *
     * @param filePath Path to the file that should be watched. This should be an absolute path.
     * @param event The event to watch for
     * @param observer FileObserver object that is to be informed about the occurrence of the event
     * @return FileEntry representing the requested watch task
     * @throws IOException If a file system related errors occur
     *
     * @see FileSystemWatch.Event
     * @see FileObserver
     */
    public FileEntry newFileEntry(String filePath, Event event, FileObserver observer) throws IOException
    {
        Path watchFile = fileSys.getPath(filePath);
        FileEntry watchEntry = new FileEntry(watchFile, event, observer, true);
        addFileEntry(watchEntry);
        return watchEntry;
    }

    /**
     * Adds the specified entry to this FileSystemWatch instance
     *
     * The entry's observer will be called upon occurrence of the entry's event
     * on the entry's file.
     *
     * Note that observer callbacks may take place before the addFileEntry() method returns.
     * Depending on when a file event is detected, the callback may be performed in the
     * context of the thread that called the newFileEntry() method or asynchronously in
     * the context of the FileSystemWatch instance's event loop thread.
     *
     * In either case, no locks are held while the callback to the observer's
     * fileEvent() method takes place.
     *
     * File creation is detected
     *   - if the file exists already when the newFileEntry() method is called
     *   - if the file is created after the newFileEntry() method returned
     *   - if the file's creation races with a call to the newFileEntry() method
     *
     * If the entry is configured to be automatically removed, then the observer will only
     * be called once; otherwise, if a CREATE or DELETE event races with the call of the
     * addFileEntry() method, the observer may be called twice for the same event.
     *
     * If the file's parent directory can not be watched because it does not exist or is
     * inaccessible, or if the file system fails to perform the necessary actions to install
     * the necessary notification services, an IOException is thrown.
     *
     * @param watchEntry FileEntry instance to add
     * @throws IOException If a file system related errors occur
     *
     * @see FileSystemWatch.Event
     * @see FileObserver
     */
    public void addFileEntry(FileEntry watchEntry) throws IOException
    {
        boolean trigger = false;
        synchronized (mapLock)
        {
            addFileEntryImpl(watchEntry);
            trigger = probeFileEntry(watchEntry);
        }
        if (trigger)
        {
            /*
             * DO NOT run this callback in the current thread
             *
             * See AbsStorProvider#waitUntilDeviceCreated for example code
             *
             * If a method creates an observer which notifies a later "syncObject.wait()" if the
             * event occurs, then this fileEvent is registered (this method) and the event
             * has already happened - if we call the observer right now, in the current thread
             * the original code will not have entered the .wait() yet when we execute the observer's
             * .notify(), thus the .wait() will never receive the .notify(). Without a timeout
             * this will hang forever.
             *
             * Calling the observer's callback method from a different thread allows the
             * original caller to make sure to first enter the .wait before a potential .notify
             * is executed.
             */
            new Thread(() -> watchEntry.fObserver.fileEvent(watchEntry)).start();
        }
    }

    /**
     * Adds all listed entries to this FileSystemWatch's instance
     * in a transaction-safe way
     * <p>
     * Either all entries will be added, or if adding an entry fails, no entries
     * will be added and none of the entries' observers will be called.
     * <p>
     * For each entry, the same rules as described for the addFileEntry()
     * method apply.
     *
     * @param entryList List of FileEntry instances to add
     * @throws IOException If a file system related errors occur
     * @see FileSystemWatch#addFileEntry
     * @see FileSystemWatch.Event
     * @see FileObserver
     */
    public void addFileEntryList(List<FileEntry> entryList) throws IOException
    {
        List<FileEntry> triggerList = new LinkedList<>();
        synchronized (mapLock)
        {
            try
            {
                // Add all file entries
                for (FileEntry entry : entryList)
                {
                    addFileEntry(entry);
                }
                // Check whether files have already been created or deleted
                for (FileEntry entry : entryList)
                {
                    if (probeFileEntry(entry))
                    {
                        triggerList.add(entry);
                    }
                }
            }
            catch (IOException ioExc)
            {
                // Adding one of the entries failed, roll back
                for (FileEntry entry : entryList)
                {
                    removeFileEntry(entry);
                }
                throw ioExc;
            }
        }
        for (FileEntry entry : triggerList)
        {
            entry.fObserver.fileEvent(entry);
        }
    }

    private void addFileEntryImpl(FileEntry watchEntry) throws IOException
    {
        Path filePath = watchEntry.watchFile;
        // TODO:
        // - Make sure parentPath is a directory
        // - Make sure parentPath is readable
        // (unless the WatchService's register() method does that anyway)
        Path watchPath = filePath.getParent();
        if (watchPath == null)
        {
            throw new IOException(
                FileSystemWatch.class +
                ": addFileEntry(): Cannot determine parent directory for path '" + filePath + "'"
            );
        }

        synchronized (mapLock)
        {
            // Add the file entry to the lookup map
            Set<FileEntry> fileMapEntries = fileMap.get(filePath);
            if (fileMapEntries == null)
            {
                fileMapEntries = new TreeSet<>();
                fileMap.put(filePath, fileMapEntries);
            }
            fileMapEntries.add(watchEntry);

            addWatcher(watchPath, watchEntry);
        }
    }

    private boolean probeFileEntry(FileEntry watchEntry)
    {
        boolean trigger = false;
        if (watchEntry.watchEvent == Event.CREATE)
        {
            File watchFile = watchEntry.watchFile.toFile();
            if (watchFile.exists())
            {
                trigger = true;
            }
        }
        else
        if (watchEntry.watchEvent == Event.DELETE)
        {
            File watchFile = watchEntry.watchFile.toFile();
            if (!watchFile.exists())
            {
                trigger = true;
            }
        }
        if (trigger && watchEntry.remove)
        {
            removeFileEntry(watchEntry);
        }
        return trigger;
    }

    /**
     * Adds a new entry that watches for file events in the specified directory
     *
     * The observer will be called upon occurrence of the specified event on files
     * in the specified directory (but not its subdirectories).
     *
     * Note that observer callbacks may take place before the newDirectoryEntry()
     * method returns.
     *
     * DirectoryEntry instances stay active until they are removed.
     *
     * If the specified directory can not be watched because it does not exist or is
     * inaccessible, or if the file system fails to perform the necessary actions to install
     * the necessary notification services, an IOException is thrown.
     *
     * No locks are held while the callback to the observer's directoryEvent() method
     * takes place.
     *
     * @param dirPath Path to the directory that should be watched. This should be an absolute path.
     * @param event The event to watch for
     * @param observer DirectoryObserver object that is to be informed about the occurrence of events
     * @return DirectoryEntry representing the requested watch task
     * @throws IOException If a file system related errors occur
     *
     * @see FileSystemWatch.Event
     * @see DirectoryObserver
     */
    public DirectoryEntry newDirectoryEntry(String dirPath, Event event, DirectoryObserver observer) throws IOException
    {
        Path watchFile = fileSys.getPath(dirPath);
        DirectoryEntry watchEntry = new DirectoryEntry(watchFile, event, observer);
        addDirectoryEntry(watchEntry);
        return watchEntry;
    }

    /**
     * Adds the specified entry to this FileSystemWatch instance
     *
     * The entry's observer will be called upon occurrence of the entry's event
     * on files in the directory specified by the entry (but not its subdirectories).
     *
     * Note that observer callbacks may take place before the newDirectoryEntry()
     * method returns.
     *
     * DirectoryEntry instances stay active until they are removed.
     *
     * If the specified directory can not be watched because it does not exist or is
     * inaccessible, or if the file system fails to perform the necessary actions to install
     * the necessary notification services, an IOException is thrown.
     *
     * No locks are held while the callback to the observer's directoryEvent() method
     * takes place.
     *
     * @param watchEntry DirectoryEntry to add
     * @throws IOException If a file system related errors occur
     *
     * @see FileSystemWatch.Event
     * @see DirectoryObserver
     */
    public void addDirectoryEntry(DirectoryEntry watchEntry) throws IOException
    {
        Path watchPath = watchEntry.watchFile;

        synchronized (mapLock)
        {
            // Add the directory to the lookup map
            Set<DirectoryEntry> dirMapEntries = dirMap.get(watchPath);
            if (dirMapEntries == null)
            {
                dirMapEntries = new TreeSet<>();
                dirMap.put(watchPath, dirMapEntries);
            }
            dirMapEntries.add(watchEntry);

            addWatcher(watchPath, watchEntry);
        }
    }

    /**
     * Removes the FileEntry from this FileSystemWatch instance
     *
     * Note that calls of the entry's FileObserver may execute asynchronously and
     * may therefore take place even after returning from this method.
     *
     * @param watchEntry The entry to remove from this FileSystemWatch instance
     */
    public void removeFileEntry(FileEntry watchEntry)
    {
        Path filePath = watchEntry.watchFile;
        Path watchPath = filePath.getParent();
        if (watchPath != null)
        {
            synchronized (mapLock)
            {
                Set<FileEntry> fileMapEntries = fileMap.get(filePath);
                if (fileMapEntries != null)
                {
                    fileMapEntries.remove(watchEntry);
                    if (fileMapEntries.isEmpty())
                    {
                        fileMap.remove(filePath);
                    }
                }

                removeWatcher(watchPath, watchEntry);
            }
        }
    }

    /**
     * Removes the DirectoryEntry from this FileSystemWatch instance
     *
     * Note that calls of the entry's DirectoryObserver may execute asynchronously and
     * may therefore take place even after returning from this method.
     *
     * @param watchEntry The entry to remove from this FileSystemWatch instance
     */
    public void removeDirectoryEntry(DirectoryEntry watchEntry)
    {
        Path watchPath = watchEntry.watchFile;

        synchronized (mapLock)
        {
            Set<DirectoryEntry> dirMapEntries = dirMap.get(watchPath);
            if (dirMapEntries != null)
            {
                dirMapEntries.remove(watchEntry);
                if (dirMapEntries.isEmpty())
                {
                    dirMap.remove(watchPath);
                }
            }
        }
    }

    private void addWatcher(Path watchPath, Entry watchEntry) throws IOException
    {
        if (Files.notExists(watchPath))
        {
            new NonExistingParentEntry(this, watchPath, watchEntry).handleNextEntry();
        }
        else
        {
            synchronized (mapLock)
            {
                // If the directory referenced by the FileEntry is not currently
                // being watched, register the directory and create a new entry
                // for it in the watchMap. Otherwise, add the FileEntry to the
                // list of watchers associated with the corresponding watchMap
                // entry.
                WatchMapEntry wMapEntry = watchMap.get(watchPath);
                if (wMapEntry == null)
                {
                    WatchKey key = watchPath.register(
                        wSvc,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_DELETE,
                        StandardWatchEventKinds.ENTRY_MODIFY
                    );
                    wMapEntry = new WatchMapEntry(key);
                    watchMap.put(watchPath, wMapEntry);
                }
                wMapEntry.entries.add(watchEntry);
            }
        }
    }

    private void removeWatcher(Path watchPath, Entry watchEntry)
    {
        synchronized (mapLock)
        {
            WatchMapEntry wMapEntry = watchMap.get(watchPath);
            if (wMapEntry != null)
            {
                wMapEntry.entries.remove(watchEntry);
                if (wMapEntry.entries.isEmpty())
                {
                    // No more watchers for this directory
                    // Cancel the directory's WatchKey
                    wMapEntry.key.cancel();
                    // Remove the corresponding entry from the watchMap
                    watchMap.remove(watchPath);
                }
            }
        }
    }

    @Override
    public ServiceName getServiceName()
    {
        return SERVICE_NAME;
    }

    @Override
    public String getServiceInfo()
    {
        return SERVICE_INFO;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return serviceInstanceName;
    }

    @Override
    public synchronized boolean isStarted()
    {
        return watchThread != null;
    }

    @Override
    public void awaitShutdown(long timeout)
        throws InterruptedException
    {
        Thread joinThr = null;
        synchronized (this)
        {
            joinThr = watchThread;
        }
        if (joinThr != null)
        {
            joinThr.join(timeout);
        }
    }

    @Override
    public synchronized void setServiceInstanceName(ServiceName instanceName)
    {
        if (instanceName == null)
        {
            serviceInstanceName = SERVICE_NAME;
        }
        else
        {
            serviceInstanceName = instanceName;
        }
        if (watchThread != null)
        {
            watchThread.setName(serviceInstanceName.getDisplayName());
        }
    }

    abstract static class Entry implements Comparable<Entry>
    {
        final long eventId;
        Path watchFile;
        Event watchEvent;

        Entry(Path pathSpec, Event eventSpec)
        {
            ErrorCheck.ctorNotNull(Entry.class, Path.class, pathSpec);
            ErrorCheck.ctorNotNull(Entry.class, Event.class, eventSpec);

            eventId = FileSystemWatch.nextEventId();
            watchFile = pathSpec.normalize();
            watchEvent = eventSpec;
        }

        public long getId()
        {
            return eventId;
        }

        public Path getFile()
        {
            return watchFile;
        }

        public Event getEvent()
        {
            return watchEvent;
        }

        @Override
        public int compareTo(Entry other)
        {
            int result = 0;
            if (eventId < other.eventId)
            {
                result = -1;
            }
            else
            if (eventId > other.eventId)
            {
                result = 1;
            }
            return result;
        }
    }

    /**
     * A FileEntry represents a file system path that is being watched for creation, deletion or changes
     */
    public static class FileEntry extends Entry
    {
        private final boolean remove;
        private final FileObserver fObserver;

        public FileEntry(Path pathSpec, Event eventSpec, FileObserver observer, boolean autoRemove)
        {
            super(pathSpec, eventSpec);
            ErrorCheck.ctorNotNull(FileEntry.class, FileObserver.class, observer);
            fObserver = observer;
            remove = autoRemove;
        }

        public FileEntry(Path file, Event event, FileObserver observer)
        {
            this(file, event, observer, true);
        }
    }

    /**
     * A DirectoryEntry represents a directory within a file system where events about files contained in
     * the specified directory are being watched
     */
    public static class DirectoryEntry extends Entry
    {
        private final DirectoryObserver dObserver;

        public DirectoryEntry(Path pathSpec, Event eventSpec, DirectoryObserver observer)
        {
            super(pathSpec, eventSpec);
            ErrorCheck.ctorNotNull(DirectoryEntry.class, DirectoryObserver.class, observer);
            dObserver = observer;
        }
    }

    /**
     * Encapsulates the logic for building and registering FileEntryGroup objects
     */
    public static class FileEntryGroupBuilder
    {
        private final List<FileEntry> entryList;
        private final FileEntryGroup group;
        private final FileSystem egbFileSys;

        public FileEntryGroupBuilder()
        {
            group = new FileEntryGroup();
            entryList = new LinkedList<>();
            egbFileSys = FileSystems.getDefault();
        }

        public void newEntry(Path file, Event event)
        {
            entryList.add(new FileEntry(file, event, group, true));
        }

        public void newEntry(String filePath, Event event)
        {
            Path file = egbFileSys.getPath(filePath);
            newEntry(file, event);
        }

        public FileEntryGroup create(EntryGroupObserver observer)
        {
            group.initialize(entryList, observer);
            return group;
        }
    }

    /**
     * A FileEntryGroup groups multiple FileEntry objects and enables waiting for
     * events to occur on all of the FileEntry objects as a group.
     *
     * A FileEntryGroupBuilder is used for creating and registering instances of the
     * FileEntryGroup class.
     */
    public static class FileEntryGroup implements FileObserver
    {
        private List<FileEntry> entryList;
        private final AtomicInteger eventCount;
        private @Nullable EntryGroupObserver groupObserver;
        private boolean loopWait = true;

        private FileEntryGroup()
        {
            eventCount = new AtomicInteger();
            entryList = Collections.emptyList();
            groupObserver = null;
        }

        private void initialize(List<FileEntry> entryListRef, EntryGroupObserver observer)
        {
            entryList = entryListRef;
            groupObserver = observer;
        }

        /**
         * Called by the associated FileWatchService instance when an event occurs on
         * one of the FileEntry objects in the FileEntryGroup
         */
        @Override
        public void fileEvent(FileEntry watchEntry)
        {
            int currentCount = eventCount.incrementAndGet();
            if (currentCount >= entryList.size())
            {
                if (groupObserver != null)
                {
                    groupObserver.entryGroupEvent(this);
                }
                synchronized (this)
                {
                    loopWait = false;
                    notifyAll();
                }
            }
        }

        public List<FileEntry> getEntryList()
        {
            return entryList;
        }

        /**
         * Waits until each entry's event has happened or until the timeout is exceeded
         *
         * @param timeout Call timeout in milliseconds
         *
         * @throws FsWatchTimeoutException If the timeout is excee\
         * @throws NegativeTimeException If the timeout is a negative value
         * @throws ValueOutOfRangeException If the target time, calculated as current time + timeout,
         *     would overflow Long.MAX_VALUE
         * @throws InterruptedException If the thread that called waitGroup() is interrupted
         */
        public void waitGroup(long timeout)
            throws NegativeTimeException, ValueOutOfRangeException, FsWatchTimeoutException, InterruptedException
        {
            if (timeout < 0)
            {
                throw new NegativeTimeException();
            }
            long targetTime;
            {
                long now = System.currentTimeMillis();
                try
                {
                    targetTime = Math.addExact(now, timeout);
                }
                catch (ArithmeticException arithExc)
                {
                    throw new ValueOutOfRangeException(ValueOutOfRangeException.ViolationType.TOO_HIGH);
                }
            }
            synchronized (this)
            {
                long waitTime = timeout;
                while (loopWait)
                {
                    wait(waitTime);
                    if (loopWait && timeout > 0)
                    {
                        long now = System.currentTimeMillis();
                        if (now >= targetTime)
                        {
                            // Timeout exceeded
                            throw new FsWatchTimeoutException();
                        }
                        else
                        {
                            long newWaitTime = targetTime - now;
                            // Protect against timer skew
                            waitTime = newWaitTime < waitTime ? newWaitTime : waitTime;
                        }
                    }
                }
            }
        }
    }

    private static class WatchMapEntry
    {
        WatchKey key;
        Set<Entry> entries;

        WatchMapEntry(WatchKey keyRef)
        {
            key = keyRef;
            entries = new TreeSet<>();
        }
    }

    private class NonExistingParentEntry implements DirectoryObserver
    {
        private final Path origWatchPath;
        private final FileSystemWatch fsw;
        private final Entry origEntry;
        private final boolean isFileEntry;
        private @Nullable DirectoryEntry lastDirWatchEntry = null;

        NonExistingParentEntry(
            FileSystemWatch fswRef,
            Path origWatchPathRef,
            Entry origEntryRef
        )
        {
            fsw = fswRef;
            origWatchPath = origWatchPathRef;
            origEntry = origEntryRef;
            isFileEntry = origEntryRef instanceof FileEntry;
        }

        public void handleNextEntry()
        {
            try
            {
                Path watchPath = origWatchPath;

                boolean useOrigEntry;

                int missingParents = 0;
                if (Files.notExists(watchPath))
                {
                    useOrigEntry = false;
                    while (Files.notExists(watchPath))
                    {
                        missingParents++;
                        watchPath = watchPath.getParent();
                    }
                }
                else
                {
                    useOrigEntry = true;
                }

                boolean fileEntryReached = missingParents == 0 && isFileEntry;
                if (Files.isDirectory(watchPath) && !fileEntryReached)
                {
                    if (useOrigEntry)
                    {
                        fsw.newDirectoryEntry(
                            watchPath.toString(),
                            origEntry.watchEvent,
                            ((DirectoryEntry) origEntry).dObserver
                        );
                    }
                    else
                    {
                        if (lastDirWatchEntry != null)
                        {
                            fsw.removeDirectoryEntry(lastDirWatchEntry);
                        }
                        lastDirWatchEntry = fsw.newDirectoryEntry(watchPath.toString(), Event.CREATE, this);
                    }
                }
                else
                {
                    if (lastDirWatchEntry != null)
                    {
                        fsw.removeDirectoryEntry(lastDirWatchEntry);
                    }
                    // if watchPath is a not a directoy, we are done - no need to check useOrigEntry
                    if (isFileEntry)
                    {
                        fsw.addFileEntry((FileEntry) origEntry);
                    }
                    else
                    {
                        fsw.addDirectoryEntry((DirectoryEntry) origEntry);
                    }
                }

            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(
                    ioExc,
                    null,
                    null,
                    "An IO exception occurred when trying to register a file watch event"
                );
            }
        }

        @Override
        public void directoryEvent(DirectoryEntry watchEntry, Path filePath)
        {
            handleNextEntry();
        }
    }

    private static synchronized long nextEventId()
    {
        long id = globalEventId;
        ++globalEventId;
        return id;
    }
}
