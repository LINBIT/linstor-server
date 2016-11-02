package com.linbit.fsevent;

import com.linbit.timer.Delay;
import com.linbit.fsevent.FileSystemWatch.FileEntry;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroup;
import com.linbit.fsevent.FileSystemWatch.FileEntryGroupBuilder;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests the FileSystemWatch class
 *
 * @author raltnoeder
 */
public class FileSystemWatchTest
{
    public static final String TEST_PATH = "test-data/drbdmanage-tests";

    private FileSystemWatch fsw;

    public FileSystemWatchTest()
    {
    }

    private void createFile(String fileName)
    {
        String filePath = fileName;
        try
        {
            FileOutputStream fOut = new FileOutputStream(filePath);
            fOut.close();
        }
        catch (IOException exc)
        {
            fail(String.format("Test failed, failed to create file '%s'", filePath));
        }
    }

    private void deleteFile(String fileName)
    {
        String filePath = fileName;
        File targetFile = new File(filePath);
        if (!targetFile.delete())
        {
            fail(String.format("Test failed, failed to delete file '%s'", filePath));
        }
    }

    private void modifyFile(String fileName)
    {
        String filePath = fileName;
        try
        {
            FileOutputStream fOut = new FileOutputStream(filePath);
            fOut.write(0);
            fOut.close();
        }
        catch (IOException exc)
        {
            fail(String.format("Test failed, failed to create file '%s'", filePath));
        }
    }

    private String testFilePath(String fileName)
    {
        StringBuilder filePath = new StringBuilder();
        filePath.append(TEST_PATH);
        if (!TEST_PATH.endsWith("/"))
        {
            filePath.append("/");
        }
        filePath.append(fileName);
        return filePath.toString();
    }

    private void fileEventCheck(FileEventReceiver rec)
    {
        if (rec.isFailed())
        {
            fail("FileObserver triggered for an unexpected event");
        }

        if (!rec.isFinished())
        {
            fail("FileObserver not triggered for all expected events");
        }
    }

    private void entryGroupCheck(EntryGroupReceiver rec, FileEntryGroup entryGroup)
    {
        if (rec.isFailed())
        {
            fail("EntryGroupObserver triggered more than once");
        }

        if (!rec.isFinished(entryGroup))
        {
            fail("EntryGroupObserver not triggered or triggered for the wrong FileEntryGroup");
        }
    }

    @Before
    public void setUp()
    {
        try
        {
            fsw = new FileSystemWatch();
            fsw.start();
        }
        catch (IOException ioExc)
        {
            fail("Test setup failed: Creation of a new FileSystemWatch failed");
        }
    }

    @After
    public void tearDown()
    {
        fsw.shutdown();
    }

    /**
     * Waits for a single file to be created
     */
    @Test
    public void singleFileCreateTest() throws Exception
    {
        String testFile = testFilePath("testfile.txt");

        FileEventReceiver rec = new FileEventReceiver();
        rec.addExpected(testFile);

        FileEntry entry = fsw.newFileEntry(testFile, FileSystemWatch.Event.CREATE, rec);

        // Create the file; should trigger the FileEventReceiver
        createFile(testFile);
        // Delete file; should not trigger the FileEventReceiver
        deleteFile(testFile);

        Delay.sleep(500L);

        fileEventCheck(rec);
    }

    /**
     * Waits for a single file to be deleted
     */
    @Test
    public void singleFileDeleteTest() throws Exception
    {
        String testFile = testFilePath("testfile.txt");

        createFile(testFile);

        FileEventReceiver rec = new FileEventReceiver();
        rec.addExpected(testFile);

        FileEntry entry = fsw.newFileEntry(testFile, FileSystemWatch.Event.DELETE, rec);

        // Modify the file; not supposed to trigger the FileEventReceiver
        modifyFile(testFile);
        // Delete file; should trigger the FileEventReceiver
        deleteFile(testFile);

        Delay.sleep(500L);

        fileEventCheck(rec);
    }

    /**
     * Waits for a single file to be modified
     */
    @Test
    public void singleFileModifyTest() throws Exception
    {
        String testFile = testFilePath("testfile.txt");

        createFile(testFile);

        FileEventReceiver rec = new FileEventReceiver();
        rec.addExpected(testFile);

        FileEntry entry = fsw.newFileEntry(testFile, FileSystemWatch.Event.CHANGE, rec);

        // Modify the file; should trigger the FileEventReceiver
        modifyFile(testFile);

        Delay.sleep(500L);

        fileEventCheck(rec);

        // Clean up
        deleteFile(testFile);
    }

    /**
     * Tests observing events with duplicate entries that refer to the same file
     *
     * The first entry is supposed to be removed automatically upon receipt of the
     * first event, while the second entry is supposed to continue receiving events.
     */
    @Test
    public void singleFileMultiEntriesTest() throws Exception
    {
        String testFile = testFilePath("testfile");

        FileMultiEventReceiver mRecOne = new FileMultiEventReceiver();
        FileMultiEventReceiver mRecTwo = new FileMultiEventReceiver();

        fsw.newFileEntry(testFile, FileSystemWatch.Event.CREATE, mRecOne);
        FileEntry entryTwo = new FileEntry(
            FileSystems.getDefault().getPath(testFile),
            FileSystemWatch.Event.CREATE,
            mRecTwo,
            false
        );
        fsw.addFileEntry(entryTwo);

        createFile(testFile);

        Delay.sleep(500L);

        if (mRecOne.isFailed())
        {
            fail("(#1) FileObserver for first FileEntry received event for different FileEntry instances");
        }
        if (mRecOne.getCallCount() != 1)
        {
            fail("(#1) FileObserver for first FileEntry did not receive the correct number of events");
        }

        deleteFile(testFile);
        createFile(testFile);

        Delay.sleep(500L);

        if (mRecOne.isFailed())
        {
            fail("(#2) FileObserver for first FileEntry received event for different FileEntry instances");
        }
        if (mRecOne.getCallCount() != 1)
        {
            fail("(#2) FileObserver for first FileEntry did not receive the correct number of events");
        }
        if (mRecTwo.isFailed())
        {
            fail("FileObserver for second FileEntry received event for different FileEntry instances");
        }
        if (mRecTwo.getCallCount() != 2)
        {
            fail("FileObserver for second FileEntry did not receive the correct number of events");
        }
    }

    /**
     * Waits for multiple files to be created
     */
    @Test
    public void multiFileCreateTest() throws Exception
    {
        String createFileOne = testFilePath("file1");
        String createFileTwo = testFilePath("file2");

        FileEntryGroupBuilder gBuilder = new FileEntryGroupBuilder();
        gBuilder.newEntry(createFileOne, FileSystemWatch.Event.CREATE);
        gBuilder.newEntry(createFileTwo, FileSystemWatch.Event.CREATE);

        EntryGroupReceiver gRec = new EntryGroupReceiver();
        FileEntryGroup entryGroup = gBuilder.create(fsw, gRec);

        createFile(createFileOne);
        if (gRec.isFinished(entryGroup))
        {
            fail("EntryGroupObserver triggered before all conditions are met");
        }
        createFile(createFileTwo);

        Delay.sleep(500L);

        entryGroupCheck(gRec, entryGroup);

        // Cleanup
        deleteFile(createFileOne);
        deleteFile(createFileTwo);
    }

    /**
     * Waits for multiple files to be deleted
     */
    @Test
    public void multiFileDeleteTest() throws Exception
    {
        String deleteFileOne = testFilePath("file1");
        String deleteFileTwo = testFilePath("file2");

        // Set up test
        // Create files for the file deletion test
        createFile(deleteFileOne);
        createFile(deleteFileTwo);

        FileEntryGroupBuilder gBuilder = new FileEntryGroupBuilder();
        gBuilder.newEntry(deleteFileOne, FileSystemWatch.Event.DELETE);
        gBuilder.newEntry(deleteFileTwo, FileSystemWatch.Event.DELETE);

        EntryGroupReceiver gRec = new EntryGroupReceiver();
        FileEntryGroup entryGroup = gBuilder.create(fsw, gRec);

        deleteFile(deleteFileOne);
        Delay.sleep(200L);
        if (gRec.isFinished(entryGroup))
        {
            fail("EntryGroupObserver triggered before all conditions are met");
        }
        deleteFile(deleteFileTwo);

        Delay.sleep(500L);

        entryGroupCheck(gRec, entryGroup);
    }

    /**
     * Waits for some files to be created and some others
     * to be deleted
     */
    @Test
    public void multiFileMixedTest() throws Exception
    {
        String createFileOne = testFilePath("file1");
        String createFileTwo = testFilePath("file2");
        String deleteFileThree = testFilePath("file3");
        String deleteFileFour = testFilePath("file4");

        // Set up test
        // Create files for the file deletion test
        createFile(deleteFileThree);
        createFile(deleteFileFour);

        FileEntryGroupBuilder gBuilder = new FileEntryGroupBuilder();
        gBuilder.newEntry(createFileOne, FileSystemWatch.Event.CREATE);
        gBuilder.newEntry(createFileTwo, FileSystemWatch.Event.CREATE);
        gBuilder.newEntry(deleteFileThree, FileSystemWatch.Event.DELETE);
        gBuilder.newEntry(deleteFileFour, FileSystemWatch.Event.DELETE);

        EntryGroupReceiver gRec = new EntryGroupReceiver();
        FileEntryGroup entryGroup = gBuilder.create(fsw, gRec);

        deleteFile(deleteFileThree);
        createFile(createFileOne);

        Delay.sleep(200L);
        if (gRec.isFinished(entryGroup))
        {
            fail("EntryGroupObserver triggered before all conditions are met");
        }

        createFile(createFileTwo);
        deleteFile(deleteFileFour);

        Delay.sleep(500L);

        entryGroupCheck(gRec, entryGroup);

        // Cleanup
        deleteFile(createFileOne);
        deleteFile(createFileTwo);
    }

    /**
     * Waits for creation of a file that is already
     * present
     */
    @Test
    public void singleExistFileCreateTest() throws Exception
    {
        String testFile = testFilePath("testfile.txt");

        // Create the file; should trigger the FileEventReceiver
        createFile(testFile);

        FileEventReceiver rec = new FileEventReceiver();
        rec.addExpected(testFile);

        FileEntry entry = fsw.newFileEntry(testFile, FileSystemWatch.Event.CREATE, rec);

        // Delete file; should not trigger the FileEventReceiver
        deleteFile(testFile);

        Delay.sleep(500L);

        fileEventCheck(rec);
    }

    /**
     * Waits for deletion of a file that does not exist
     * in the first place
     */
    @Test
    public void singleNonExistFileDeleteTest() throws Exception
    {
        String testFile = testFilePath("testfile.txt");

        FileEventReceiver rec = new FileEventReceiver();
        rec.addExpected(testFile);

        FileEntry entry = fsw.newFileEntry(testFile, FileSystemWatch.Event.DELETE, rec);

        Delay.sleep(500L);

        fileEventCheck(rec);
    }

    /**
     * Waits for creation & deletion of multiple files
     * waitFor() is called after a delay so all of the files
     * will already be present when the wait begins
     */
    @Test
    public void multiFileDelayedTest() throws Exception
    {
        String createFileOne = testFilePath("file1");
        String createFileTwo = testFilePath("file2");
        String deleteFileThree = testFilePath("file3");
        String deleteFileFour = testFilePath("file4");

        // Set up test
        // Create files for the file deletion test
        createFile(deleteFileThree);
        createFile(deleteFileFour);

        FileEntryGroupBuilder gBuilder = new FileEntryGroupBuilder();
        gBuilder.newEntry(createFileOne, FileSystemWatch.Event.CREATE);
        gBuilder.newEntry(createFileTwo, FileSystemWatch.Event.CREATE);
        gBuilder.newEntry(deleteFileThree, FileSystemWatch.Event.DELETE);
        gBuilder.newEntry(deleteFileFour, FileSystemWatch.Event.DELETE);

        EntryGroupReceiver gRec = new EntryGroupReceiver();
        final FileEntryGroup entryGroup = gBuilder.create(fsw, gRec);

        deleteFile(deleteFileThree);
        createFile(createFileOne);

        Delay.sleep(200L);
        if (gRec.isFinished(entryGroup))
        {
            fail("EntryGroupObserver triggered before all conditions are met");
        }

        createFile(createFileTwo);
        deleteFile(deleteFileFour);

        final AtomicBoolean flag = new AtomicBoolean();
        flag.set(false);
        new Thread(
            new Runnable()
            {
                public void run()
                {
                    try
                    {
                        entryGroup.waitGroup();
                        flag.set(true);
                    }
                    catch (InterruptedException ignored)
                    {
                    }
                }
            }
        ).start();

        Delay.sleep(500L);

        if (!flag.get())
        {
            fail("entryGroup.waitGroup() should have returned, but is stuck");
        }

        // Cleanup
        deleteFile(createFileOne);
        deleteFile(createFileTwo);
    }

    /**
     * Waits for creation & deletion of multiple files
     * waitFor() is called after a delay so all of the files
     * will already be present when the wait begins
     */
    @Test
    public void multiFileConcurrentTest() throws Exception
    {
        String createFileOne = testFilePath("file1");
        String createFileTwo = testFilePath("file2");
        String deleteFileThree = testFilePath("file3");
        String deleteFileFour = testFilePath("file4");

        // Set up test
        // Create files for the file deletion test
        createFile(deleteFileThree);
        createFile(deleteFileFour);

        FileEntryGroupBuilder gBuilder = new FileEntryGroupBuilder();
        gBuilder.newEntry(createFileOne, FileSystemWatch.Event.CREATE);
        gBuilder.newEntry(createFileTwo, FileSystemWatch.Event.CREATE);
        gBuilder.newEntry(deleteFileThree, FileSystemWatch.Event.DELETE);
        gBuilder.newEntry(deleteFileFour, FileSystemWatch.Event.DELETE);

        EntryGroupReceiver gRec = new EntryGroupReceiver();
        final FileEntryGroup entryGroup = gBuilder.create(fsw, gRec);

        final AtomicBoolean flag = new AtomicBoolean();
        flag.set(false);
        new Thread(
            new Runnable()
            {
                public void run()
                {
                    try
                    {
                        entryGroup.waitGroup();
                        flag.set(true);
                    }
                    catch (InterruptedException ignored)
                    {
                    }
                }
            }
        ).start();

        deleteFile(deleteFileThree);
        createFile(createFileOne);

        Delay.sleep(200L);
        if (gRec.isFinished(entryGroup))
        {
            fail("EntryGroupObserver triggered before all conditions are met");
        }
        if (flag.get())
        {
            fail("FileEntryGroup.waitGroup() returned before all conditions where met");
        }

        createFile(createFileTwo);
        deleteFile(deleteFileFour);

        Delay.sleep(500L);

        if (!flag.get())
        {
            fail("entryGroup.waitGroup() should have returned, but is stuck");
        }

        // Cleanup
        deleteFile(createFileOne);
        deleteFile(createFileTwo);
    }

    /**
     * Tests the rollback when adding on of the entries for a new FileEntryGroup fails
     */
    @Test(expected=IOException.class)
    public void multiFileRollbackTest() throws IOException
    {
        FileEntryGroupBuilder gBuilder = new FileEntryGroupBuilder();

        String createFileOne = testFilePath("file1");
        String deleteFileTwo = testFilePath("file2");
        String createFileThree = testFilePath("file3");
        String brokenPathFileOne = "/this/path/should/not/exist";
        String deleteFileFour = testFilePath("file4");
        String brokenPathFileTwo = "/this/path/should/not/exist";
        String createFileFive = testFilePath("file5");

        gBuilder.newEntry(createFileOne, FileSystemWatch.Event.CREATE);
        gBuilder.newEntry(deleteFileTwo, FileSystemWatch.Event.DELETE);
        gBuilder.newEntry(createFileThree, FileSystemWatch.Event.CREATE);
        gBuilder.newEntry(brokenPathFileOne, FileSystemWatch.Event.DELETE);
        gBuilder.newEntry(deleteFileFour, FileSystemWatch.Event.DELETE);
        gBuilder.newEntry(brokenPathFileTwo, FileSystemWatch.Event.CREATE);
        gBuilder.newEntry(createFileFive, FileSystemWatch.Event.CREATE);

        EntryGroupReceiver gRec = new EntryGroupReceiver();
        final FileEntryGroup entryGroup = gBuilder.create(fsw, gRec);
    }

    /**
     * Attempts to watch a directory that does not exist
     */
    @Test(expected=IOException.class)
    public void watchNonexistentDirectoryTest() throws Exception
    {
        FileEntry testEntry = fsw.newFileEntry(
            "/this/path/should/not/exist", FileSystemWatch.Event.CREATE,
            new FileEventReceiver()
        );
    }

    private static class FileEventReceiver implements FileObserver
    {
        Set<String> expected = new TreeSet<>();
        boolean failed = false;

        public void addExpected(String expPath)
        {
            expected.add(expPath);
        }

        @Override
        public void fileEvent(FileSystemWatch.FileEntry watchEntry)
        {
            String filePath = watchEntry.watchFile.toString();
            if (expected.contains(filePath))
            {
                expected.remove(filePath);
            }
            else
            {
                failed = true;
            }
        }

        public boolean isFinished()
        {
            return expected.isEmpty();
        }

        public boolean isFailed()
        {
            return failed;
        }
    }

    private static class FileMultiEventReceiver implements FileObserver
    {
        FileEntry entry = null;
        AtomicInteger callCount = new AtomicInteger();
        boolean failed = false;

        @Override
        public void fileEvent(FileEntry watchEntry)
        {
            if (entry == null)
            {
                entry = watchEntry;
            }

            if (entry == watchEntry)
            {
                callCount.incrementAndGet();
            }
            else
            {
                failed = true;
            }
        }

        public boolean isFailed()
        {
            return failed;
        }

        public int getCallCount()
        {
            return callCount.get();
        }
    }

    private static class EntryGroupReceiver implements EntryGroupObserver
    {
        FileEntryGroup entryGroup = null;
        boolean failed = false;

        @Override
        public void entryGroupEvent(FileEntryGroup group)
        {
            if (entryGroup == null)
            {
                entryGroup = group;
            }
            else
            {
                failed = true;
            }
        }

        public boolean isFailed()
        {
            return failed;
        }

        public boolean isFinished(FileEntryGroup other)
        {
            return entryGroup == other;
        }
    }

}
