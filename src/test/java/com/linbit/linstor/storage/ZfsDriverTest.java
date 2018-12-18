package com.linbit.linstor.storage;

//import static com.linbit.linstor.storage.StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY;
//import static com.linbit.linstor.storage.StorageConstants.CONFIG_ZFS_COMMAND_KEY;
//import static com.linbit.linstor.storage.StorageConstants.CONFIG_ZFS_POOL_KEY;
//import static com.linbit.linstor.storage.ZfsDriver.ZFS_COMMAND_DEFAULT;
//import static com.linbit.linstor.storage.ZfsDriver.ZFS_POOL_COMMAND_DEFAULT;
//import static com.linbit.linstor.storage.ZfsDriver.ZFS_POOL_DEFAULT;
//import static com.linbit.linstor.storage.ZfsDriver.ZFS_VOLBLOCKSIZE;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Paths;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//
//import com.linbit.linstor.api.ApiConsts;
//import com.linbit.linstor.propscon.ReadOnlyProps;
//
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import com.linbit.drbd.md.MetaData;
//import com.linbit.extproc.ExtCmd;
//import com.linbit.extproc.ExtCmd.OutputData;
//import com.linbit.extproc.utils.TestExtCmd.Command;
//import com.linbit.extproc.utils.TestExtCmd.TestOutputData;
//import com.linbit.fsevent.FileSystemWatch.Event;
//import com.linbit.fsevent.FileSystemWatch.FileEntry;
//import com.linbit.fsevent.FileSystemWatch.FileEntryGroup;
//import com.linbit.fsevent.FileSystemWatch.FileEntryGroupBuilder;
//
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({
//    ZfsDriver.class,
//    ExtCmd.class
//})
//public class ZfsDriverTest extends StorageTestUtils
//{
//    private static final String STOR_POOL_NAME = "TestStorPool";
//    private static final long MB = 1024;
//    private static final long TEST_SIZE_100MB = 100 * MB;
//    private static final long TEST_EXTENT_SIZE = ZFS_VOLBLOCKSIZE;
//    private static final long TEST_TOLERANCE_FACTOR = 4;
//    private static final long CREATE_VOL_WAIT_TIME = 2000;
//
//    private static final Map<String, String> EMPTY_MAP = Collections.emptyMap();
//
//    public ZfsDriverTest() throws Exception
//    {
//        super(new ZfsDriverKind());
//    }
//
//    @Test
//    public void testConfigPool() throws StorageException
//    {
//        HashMap<String, String> config = new HashMap<>();
//
//        String poolName = "otherName";
//        config.put(CONFIG_ZFS_POOL_KEY, poolName);
//        expectCheckPoolName(ZFS_COMMAND_DEFAULT, poolName);
//        driver.setConfiguration(STOR_POOL_NAME, config, EMPTY_MAP, EMPTY_MAP);
//
//        ec.clearBehaviors();
//        poolName = "_specialName";
//        config.put(CONFIG_ZFS_POOL_KEY, poolName);
//        expectCheckPoolName(ZFS_COMMAND_DEFAULT, poolName);
//        driver.setConfiguration(STOR_POOL_NAME, config, EMPTY_MAP, EMPTY_MAP);
//
//        ec.clearBehaviors();
//        poolName = "special-Name";
//        config.put(CONFIG_ZFS_POOL_KEY, poolName);
//        expectCheckPoolName(ZFS_COMMAND_DEFAULT, poolName);
//        driver.setConfiguration(STOR_POOL_NAME, config, EMPTY_MAP, EMPTY_MAP);
//    }
//
//    @Test(expected = StorageException.class)
//    public void testConfigPoolValidNotExistent() throws StorageException
//    {
//        String pool = "valid";
//        Map<String, String> config = createMap(CONFIG_ZFS_POOL_KEY, pool);
//        expectCheckPoolName(ZFS_COMMAND_DEFAULT, pool, false);
//        driver.setConfiguration(STOR_POOL_NAME, config, EMPTY_MAP, EMPTY_MAP);
//    }
//
//    @Test(expected = StorageException.class)
//    public void testConfigVolumeGroupEmpty() throws StorageException
//    {
//        driver.setConfiguration(STOR_POOL_NAME, createMap(CONFIG_ZFS_POOL_KEY, ""), EMPTY_MAP, EMPTY_MAP);
//    }
//
//    @Test(expected = StorageException.class)
//    public void testConfigVolumeGroupWhitespacesOnly() throws StorageException
//    {
//        driver.setConfiguration(STOR_POOL_NAME, createMap(CONFIG_ZFS_POOL_KEY, "  "), EMPTY_MAP, EMPTY_MAP);
//    }
//
//    @Test
//    public void testConfigCommand() throws StorageException, IOException
//    {
//        expectException(createMap(CONFIG_ZFS_COMMAND_KEY, "not-zfs"));
//
//        String zfsCommand = "otherzfs";
//        File tmpFile = tempFolder.newFile(zfsCommand);
//        tmpFile.setExecutable(true);
//        expectCheckPoolName(tmpFile.getAbsolutePath(), ZFS_POOL_DEFAULT);
//        driver.setConfiguration(
//            STOR_POOL_NAME,
//            createMap(CONFIG_ZFS_COMMAND_KEY, tmpFile.getAbsolutePath()),
//            EMPTY_MAP,
//            EMPTY_MAP
//        );
//
//        String pool = "newPool";
//        expectCheckPoolName(tmpFile.getAbsolutePath(), pool);
//        driver.setConfiguration(
//            STOR_POOL_NAME,
//            createMap(CONFIG_ZFS_POOL_KEY, pool),
//            EMPTY_MAP,
//            EMPTY_MAP
//        );
//    }
//
//    @Test
//    public void testConfigToleranceFactor() throws StorageException
//    {
//        expectCheckPoolName(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT);
//        expectException(createMap(CONFIG_SIZE_ALIGN_TOLERANCE_KEY, "2.4"));
//        expectException(createMap(CONFIG_SIZE_ALIGN_TOLERANCE_KEY, "0"));
//        expectException(createMap(CONFIG_SIZE_ALIGN_TOLERANCE_KEY, "-1"));
//        expectException(createMap(CONFIG_SIZE_ALIGN_TOLERANCE_KEY, "NaN"));
//
//        driver.setConfiguration(
//            STOR_POOL_NAME,
//            createMap(CONFIG_SIZE_ALIGN_TOLERANCE_KEY, Long.toString(TEST_TOLERANCE_FACTOR)),
//            EMPTY_MAP,
//            EMPTY_MAP
//        );
//
//        String identifier = "identifier";
//
//        long size = TEST_SIZE_100MB;
//        long zfsExtent = TEST_EXTENT_SIZE;
//        expectZfsVolumeInfoBehavior(
//            ZFS_COMMAND_DEFAULT,
//            ZFS_POOL_DEFAULT,
//            identifier,
//            size + zfsExtent * TEST_TOLERANCE_FACTOR
//        );
//        expectZfsExtentCommand(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, zfsExtent);
//
//        {
//            StorageDriver.SizeComparison sizeComparison = driver.compareVolumeSize(
//                identifier,
//                TEST_SIZE_100MB,
//                ReadOnlyProps.emptyRoProps()
//            );
//            assertEquals(
//                "volume size should be in tolerance",
//                StorageDriver.SizeComparison.WITHIN_TOLERANCE,
//                sizeComparison
//            );
//        }
//
//        expectZfsVolumeInfoBehavior(
//            ZFS_COMMAND_DEFAULT,
//            ZFS_POOL_DEFAULT,
//            identifier,
//            size + zfsExtent * TEST_TOLERANCE_FACTOR + 1
//        );
//        // TODO: add comments what is going on here
//
//        try
//        {
//            StorageDriver.SizeComparison sizeComparison = driver.compareVolumeSize(
//                identifier,
//                TEST_SIZE_100MB,
//                ReadOnlyProps.emptyRoProps()
//            );
//            assertEquals(
//                "volume size should be higher than tolerated",
//                StorageDriver.SizeComparison.TOO_LARGE,
//                sizeComparison
//            );
//        }
//        catch (StorageException storExc)
//        {
//            // expected
//        }
//    }
//
//    @Test
//    public void testStartVolume() throws StorageException
//    {
//        String identifier = "identifier";
//        driver.startVolume(identifier, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//    @Test
//    public void testStartUnknownVolume() throws StorageException
//    {
//        String unknownIdentifier = "unknown";
//        driver.startVolume(unknownIdentifier, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//    @Test
//    public void testStopVolume() throws StorageException
//    {
//        String identifier = "identifier";
//        driver.stopVolume(identifier, false, ReadOnlyProps.emptyRoProps()); // should not trigger anything
//    }
//
//    @Test
//    public void testStopUnknownVolume() throws StorageException
//    {
//        String unknownIdentifier = "unknown";
//        driver.stopVolume(unknownIdentifier, false, ReadOnlyProps.emptyRoProps()); // should not trigger anything
//    }
//
//
//    @Test
//
//    public void testCreateVolumeDelayed() throws Exception
//    {
//        long volumeSize = TEST_SIZE_100MB; // size in KiB => 100MB
//        String identifier = "testVolume";
//
//        expectZfsExtentCommand(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, TEST_EXTENT_SIZE);
//        expectZfsVolumeInfoBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, volumeSize);
//        expectZfsCreateVolumeBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, volumeSize, identifier);
//
//        String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
//            expectedFilePath,
//            Event.CREATE,
//            testFileEntryGroup
//        );
//        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);
//
//        final FileEntry testEntry = new FileEntry(
//            Paths.get(expectedFilePath),
//            Event.CREATE,
//            emptyFileObserver
//        );
//
//        Thread thread = new Thread(
//            new Runnable()
//            {
//                @Override
//                public void run()
//                {
//                    try
//                    {
//                        Thread.sleep(CREATE_VOL_WAIT_TIME); // give the driver some time to execute the
//                        // .createVolume command
//                    }
//                    catch (InterruptedException exc)
//                    {
//                        exc.printStackTrace();
//                    }
//                    testFileEntryGroup.fileEvent(testEntry);
//                }
//            }
//        );
//        thread.start();
//        driver.createVolume(identifier, volumeSize, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//    @Test
//
//    public void testCreateVolumeInstant() throws Exception
//    {
//        long volumeSize = TEST_SIZE_100MB; // size in KiB => 100MB
//        String identifier = "testVolume";
//
//        expectZfsExtentCommand(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, TEST_EXTENT_SIZE);
//        expectZfsVolumeInfoBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, volumeSize);
//        expectZfsCreateVolumeBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, volumeSize, identifier);
//
//        String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
//            expectedFilePath,
//            Event.CREATE,
//            testFileEntryGroup
//        );
//        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);
//
//        FileEntry testEntry = new FileEntry(
//            Paths.get(expectedFilePath),
//            Event.CREATE,
//            emptyFileObserver);
//
//        testFileEntryGroup.fileEvent(testEntry);
//        driver.createVolume(identifier, volumeSize, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//    @Test(expected = StorageException.class)
//
//    public void testCreateVolumeTimeout() throws Exception
//    {
//        long volumeSize = TEST_SIZE_100MB; // size in KiB => 100MB
//        String identifier = "testVolume";
//
//        expectZfsExtentCommand(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, TEST_EXTENT_SIZE);
//        expectZfsCreateVolumeBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, volumeSize, identifier);
//
//        String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
//            expectedFilePath,
//            Event.CREATE,
//            testFileEntryGroup
//        );
//        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);
//
//        // do not fire file event --> timeout
//        driver.createVolume(identifier, volumeSize, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//
//    @Test(expected = StorageException.class)
//
//    public void testCreateExistingVolume() throws Exception
//    {
//        long volumeSize = TEST_SIZE_100MB; // size in KiB => 100MB
//        String identifier = "testVolume";
//
//        expectZfsExtentCommand(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, TEST_EXTENT_SIZE);
//        expectZfsCreateVolumeBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, volumeSize, identifier, true);
//        driver.createVolume(identifier, volumeSize, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//    @Test
//
//    public void testCreateAutoCorrectSize() throws Exception
//    {
//        long volumeSize = TEST_SIZE_100MB + 1; // this should be rounded up to
//        long correctedSize = TEST_SIZE_100MB + TEST_EXTENT_SIZE;
//        String identifier = "testVolume";
//
//        expectZfsExtentCommand(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, TEST_EXTENT_SIZE);
//        expectZfsCreateVolumeBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, correctedSize, identifier);
//        expectZfsVolumeInfoBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, correctedSize);
//
//
//        String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
//            expectedFilePath,
//            Event.CREATE,
//            testFileEntryGroup
//        );
//        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);
//
//        FileEntry testEntry = new FileEntry(
//            Paths.get(expectedFilePath),
//            Event.CREATE,
//            emptyFileObserver);
//
//        testFileEntryGroup.fileEvent(testEntry);
//
//        driver.createVolume(identifier, volumeSize, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//    @Test
//    public void testDeleteVolume() throws Exception
//    {
//        String identifier = "testVolume";
//
//        expectZfsDeleteVolumeBehavior(ZFS_COMMAND_DEFAULT, identifier, ZFS_POOL_DEFAULT);
//
//        String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
//            expectedFilePath,
//            Event.DELETE,
//            testFileEntryGroup
//        );
//        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);
//
//        FileEntry testEntry = new FileEntry(
//            Paths.get(expectedFilePath),
//            Event.DELETE,
//            emptyFileObserver
//        );
//
//        testFileEntryGroup.fileEvent(testEntry);
//        driver.deleteVolume(identifier, false, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test(expected = StorageException.class)
//    public void testDeleteVolumeTimeout() throws Exception
//    {
//        String identifier = "testVolume";
//
//        expectZfsDeleteVolumeBehavior(ZFS_COMMAND_DEFAULT, identifier, ZFS_POOL_DEFAULT);
//
//        String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
//            expectedFilePath,
//            Event.DELETE,
//            testFileEntryGroup
//        );
//        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);
//
//        // do not fire file event
//        driver.deleteVolume(identifier, false, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test(expected = StorageException.class)
//    public void testDeleteVolumeNonExisting() throws Exception
//    {
//        String identifier = "testVolume";
//
//        expectZfsDeleteVolumeBehavior(ZFS_COMMAND_DEFAULT, identifier, ZFS_POOL_DEFAULT, false);
//        expectZfsVolumeExistsBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier);
//
//        driver.deleteVolume(identifier, false, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test
//
//    public void testCheckVolume() throws StorageException
//    {
//        String identifier = "testVolume";
//        long size = TEST_SIZE_100MB; // size in KiB => 100MB
//        long zfsExtent = TEST_EXTENT_SIZE;
//
//        expectZfsVolumeInfoBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, size);
//        expectZfsExtentCommand(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, zfsExtent);
//
//        driver.compareVolumeSize(identifier, size, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test(expected = StorageException.class)
//    public void testCheckVolumeSizeTooLarge() throws StorageException
//    {
//        String identifier = "testVolume";
//        long size = MetaData.DRBD_MAX_kiB + 1;
//
//        driver.compareVolumeSize(identifier, size, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test
//    @SuppressWarnings("checkstyle:magicnumber")
//    public void testCheckVolumeTooSmall() throws StorageException
//    {
//        String identifier = "testVolume";
//        long size = TEST_SIZE_100MB; // size in KiB => 100MB
//
//        expectZfsExtentCommand(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, TEST_EXTENT_SIZE);
//        expectZfsVolumeInfoBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, size - 10);
//        // user wanted at least 100 MB, but we give him a little bit less
//
//        StorageDriver.SizeComparison sizeComparison = driver.compareVolumeSize(
//            identifier,
//            size,
//            ReadOnlyProps.emptyRoProps()
//        );
//        assertEquals(
//            "volume size should be too small",
//            StorageDriver.SizeComparison.TOO_SMALL,
//            sizeComparison
//        );
//    }
//
//    @Test
//    public void testVolumePath() throws StorageException
//    {
//        String identifier = "testVolume";
//        long size = TEST_SIZE_100MB; // size in KiB => 100MB
//
//        expectZfsVolumeInfoBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, size);
//
//        final String path = driver.getVolumePath(identifier, false, ReadOnlyProps.emptyRoProps());
//        assertEquals(
//            "/dev/zvol/" +
//            ZFS_POOL_DEFAULT + "/" +
//            identifier,
//            path
//        );
//    }
//
//    @Test(expected = StorageException.class)
//    public void testVolumePathUnknownVolume() throws StorageException
//    {
//        String identifier = "testVolume";
//        long size = TEST_SIZE_100MB; // size in KiB => 100MB
//
//        expectZfsVolumeInfoBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, size, false);
//
//        driver.getVolumePath(identifier, false, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test
//    public void testSize() throws StorageException
//    {
//        String identifier = "testVolume";
//        long expectedSize = TEST_SIZE_100MB; // size in KiB => 100MB
//
//        expectZfsVolumeInfoBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, expectedSize);
//
//        final long size = driver.getSize(identifier, ReadOnlyProps.emptyRoProps());
//        assertEquals(expectedSize, size);
//    }
//
//    @Test(expected = StorageException.class)
//    public void testSizeUnknownVolume() throws StorageException
//    {
//        String identifier = "testVolume";
//        long size = TEST_SIZE_100MB; // size in KiB => 100MB
//
//        expectZfsVolumeInfoBehavior(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, identifier, size, false);
//
//        driver.getSize(identifier, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test
//    public void testFreeSpace() throws StorageException
//    {
//        final long size = 1L * 1024 * 1024 * 1024; // 1TB
//        expectZfsFreeSpaceCommand(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, null, size, true);
//
//        assertEquals(size, driver.getFreeSpace());
//    }
//
//    @Test
//    public void testTotalSpace() throws StorageException
//    {
//        final long size = 1L * 1024 * 1024; // 1GiB
//        expectZfsTotalSpaceCommand(ZFS_POOL_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, size, true);
//
//        assertEquals(size, driver.getTotalSpace());
//    }
//
//    @Test
//    public void testTraits() throws StorageException
//    {
//        expectZfsExtentCommand(ZFS_COMMAND_DEFAULT, ZFS_POOL_DEFAULT, "testVolume", TEST_EXTENT_SIZE);
//        Map<String, String> traits = driver.getTraits("testVolume");
//
//        final String size = traits.get(ApiConsts.KEY_STOR_POOL_ALLOCATION_UNIT);
//        assertEquals(String.valueOf(ZFS_VOLBLOCKSIZE), size);
//    }
//
//    @Test
//    public void testStaticTraits()
//    {
//        Map<String, String> traits = driver.getKind().getStaticTraits();
//
//        final String traitProv = traits.get(ApiConsts.KEY_STOR_POOL_PROVISIONING);
//        assertEquals(ApiConsts.VAL_STOR_POOL_PROVISIONING_FAT, traitProv);
//    }
//
//    @Test
//    public void testConfigurationKeys()
//    {
//        final HashSet<String> keys = new HashSet<>(driver.getKind().getConfigurationKeys());
//
//        assertTrue(keys.remove(StorageConstants.CONFIG_ZFS_COMMAND_KEY));
//        assertTrue(keys.remove(StorageConstants.CONFIG_ZFS_POOL_KEY));
//        assertTrue(keys.remove(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY));
//
//        assertTrue(keys.isEmpty());
//    }
//
//
//    protected void expectCheckPoolName(String zfsCommand, String pool)
//    {
//        expectCheckPoolName(zfsCommand, pool, true);
//    }
//
//    protected void expectCheckPoolName(String zfsCommand, String pool, boolean poolExists)
//    {
//        Command command = new Command(
//            zfsCommand,
//            "list",
//            "-H",
//            "-o", "name",
//            pool
//        );
//        OutputData outData;
//        if (poolExists)
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                pool,
//                "",
//                0
//            );
//        }
//        else
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "",
//                "cannot open '" + pool + "': dataset does not exist",
//                1
//            );
//        }
//        ec.setExpectedBehavior(command, outData);
//    }
//
//    protected void expectZfsVolumeInfoBehavior(
//        String zfsCommand,
//        String pool,
//        String identifier,
//        long size
//    )
//    {
//        expectZfsVolumeInfoBehavior(zfsCommand, pool, identifier, size, true);
//    }
//
//    protected  void expectZfsVolumeExistsBehavior(
//        String zfsCommand,
//        String pool,
//        String identifier
//    )
//    {
//        Command command = new Command(
//            zfsCommand,
//            "list",
//            "-Hp", // no headers, parsable
//            "-t", "volume", // type volume
//            pool + "/" + identifier // the specified volume
//        );
//
//        OutputData outData = new TestOutputData(command.getRawCommand(),"", "", 0);
//        ec.setExpectedBehavior(command, outData);
//    }
//
//    protected void expectZfsVolumeInfoBehavior(
//        String zfsCommand,
//        String pool,
//        String identifier,
//        long size,
//        boolean poolExists
//    )
//    {
//        Command command = new Command(
//            zfsCommand,
//            "list",
//            "-H", // no headers
//            "-p", // parsable version, tab spaced, in bytes
//            "-o", "volsize,used", // print specified columns only
//            pool + "/" + identifier // the specified volume
//        );
//        OutputData outData;
//        if (poolExists)
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                // volsize
//                size * MB + "\t" +
//                    // used
//                    size * MB / 2,
//                "",
//                0
//            );
//        }
//        else
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "",
//                "cannot open '" + pool + "/" + identifier + "': dataset does not exist",
//                1
//            );
//        }
//        ec.setExpectedBehavior(command, outData);
//    }
//
//
//    protected void expectZfsExtentCommand(
//        String zfsCommand,
//        String pool,
//        final String identifier,
//        long size
//    )
//    {
//        expectZfsExtentCommand(zfsCommand, pool, identifier, size, true);
//    }
//
//    protected void expectZfsExtentCommand(
//        String zfsCommand,
//        String pool,
//        final String identifier,
//        long size,
//        boolean poolExists
//    )
//    {
//        expectZfsStorageVolumeExistsCommand(zfsCommand, pool, identifier, poolExists);
//        if (poolExists)
//        {
//            expectZfsSizeCommand(zfsCommand, "volblocksize", pool, identifier, size, poolExists);
//        }
//    }
//
//    protected void expectZfsFreeSpaceCommand(
//        String zfsCommand,
//        String pool,
//        final String identifier,
//        long size,
//        boolean poolExists
//    )
//    {
//        expectZfsSizeCommand(zfsCommand, "available", pool, null, size, poolExists);
//    }
//
//    protected void expectZfsStorageVolumeExistsCommand(
//        final String zfsCommand,
//        final String pool,
//        final String identifier,
//        boolean exists
//    )
//    {
//        Command command = new Command(
//            zfsCommand,
//            "list",
//            "-Hp",
//            "-t", "volume",
//            pool + '/' + identifier
//        );
//        OutputData outData = new TestOutputData(
//            command.getRawCommand(),
//            "",
//            "",
//            exists ? 0 : 1
//        );
//        ec.setExpectedBehavior(command, outData);
//    }
//
//    protected void expectZfsTotalSpaceCommand(
//        final String zfsCommand,
//        final String pool,
//        long size,
//        boolean poolExists
//    )
//    {
//        final String property = "size";
//        Command command = new Command(
//            zfsCommand,
//            "get", property,
//            "-Hp",
//            pool
//        );
//        OutputData outData;
//        if (poolExists)
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                pool + "\t" + property + "\t" + Long.toString(size * MB) + "\t" + "-",
//                "",
//                0
//            );
//        }
//        else
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "",
//                "cannot open '" + pool + "': dataset does not exist",
//                1
//            );
//        }
//
//        ec.setExpectedBehavior(command, outData);
//    }
//
//    protected void expectZfsSizeCommand(
//        final String zfsCommand,
//        final String property,
//        final String pool,
//        final String identifier,
//        long size,
//        boolean poolExists
//    )
//    {
//        String poolId = pool;
//        if (identifier != null)
//        {
//            poolId += '/' + identifier;
//        }
//        Command command = new Command(
//            zfsCommand,
//            "get", property,
//            "-o", "value",
//            "-Hp",
//            poolId
//        );
//        OutputData outData;
//        if (poolExists)
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                Long.toString(size * MB),
//                "",
//                0
//            );
//        }
//        else
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "",
//                "cannot open '" + pool + "': dataset does not exist",
//                1
//            );
//        }
//
//        ec.setExpectedBehavior(command, outData);
//    }
//
//    protected void expectZfsCreateVolumeBehavior(
//        String zfsCommand,
//        String pool,
//        long size,
//        String identifier
//    )
//    {
//        expectZfsCreateVolumeBehavior(zfsCommand, pool, size, identifier, false);
//    }
//
//    protected void expectZfsCreateVolumeBehavior(
//        String zfsCommand,
//        String pool,
//        long size,
//        String identifier,
//        boolean volumeExists
//    )
//    {
//        Command command = new Command(
//            zfsCommand,
//            "create",
//            "-V", size + "KB",
//            pool + "/" + identifier
//        );
//        OutputData outData;
//        if (!volumeExists)
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "",
//                "",
//                0
//            );
//        }
//        else
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "",
//                "cannot create '" + pool + "/" + identifier + "': dataset already exists",
//                1
//            );
//        }
//
//        ec.setExpectedBehavior(command, outData);
//    }
//
//    private void expectZfsDeleteVolumeBehavior(
//        String zfsCommand,
//        String identifier,
//        String pool
//    )
//    {
//        expectZfsDeleteVolumeBehavior(zfsCommand, identifier, pool, true);
//    }
//
//    private void expectZfsDeleteVolumeBehavior(
//        String zfsCommand,
//        String identifier,
//        String pool,
//        boolean volumeExists
//    )
//    {
//        Command command = new Command(
//            zfsCommand,
//            "destroy",
//            pool + "/" + identifier
//        );
//        OutputData outData;
//        if (volumeExists)
//        {
//           outData = new TestOutputData(
//               command.getRawCommand(),
//               "",
//               "",
//               0
//           );
//        }
//        else
//        {
//            outData = new TestOutputData(
//                command.getRawCommand(),
//                "",
//                "cannot open '" + pool + "/identifier" + "': dataset does not exist",
//                1
//            );
//        }
//        ec.setExpectedBehavior(command, outData);
//    }
//}
