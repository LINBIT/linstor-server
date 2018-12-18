package com.linbit.linstor.storage;

//import static com.linbit.linstor.storage.LvmDriver.LVM_CREATE_DEFAULT;
//import static com.linbit.linstor.storage.LvmDriver.LVM_LVS_DEFAULT;
//import static com.linbit.linstor.storage.LvmDriver.LVM_REMOVE_DEFAULT;
//import static com.linbit.linstor.storage.LvmDriver.LVM_VGS_DEFAULT;
//import static com.linbit.linstor.storage.LvmDriver.LVM_VOLUME_GROUP_DEFAULT;
//import static com.linbit.linstor.storage.StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY;
//import static com.linbit.linstor.storage.StorageConstants.CONFIG_LVM_CHANGE_COMMAND_KEY;
//import static com.linbit.linstor.storage.StorageConstants.CONFIG_LVM_CREATE_COMMAND_KEY;
//import static com.linbit.linstor.storage.StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY;
//import static com.linbit.linstor.storage.StorageConstants.CONFIG_LVM_REMOVE_COMMAND_KEY;
//import static com.linbit.linstor.storage.StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY;
//import static com.linbit.linstor.storage.StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY;
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
//import com.linbit.fsevent.FileSystemWatch.Event;
//import com.linbit.fsevent.FileSystemWatch.FileEntry;
//import com.linbit.fsevent.FileSystemWatch.FileEntryGroup;
//import com.linbit.fsevent.FileSystemWatch.FileEntryGroupBuilder;
//
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({
//    LvmDriver.class,
//    ExtCmd.class
//})
//public class LvmDriverTest extends StorageTestUtils
//{
//    protected static final String EXT_COMMAND_SEPARATOR = ";";
//
//    private static final String STOR_POOL_NAME = "TestStorPool";
//    private static final long MB = 1024;
//    private static final long TEST_SIZE_100MB = 100 * MB;
//    private static final long TEST_EXTENT_SIZE = 4096;
//    private static final long TEST_TOLERANCE_FACTOR = 4;
//    private static final long CREATE_VOL_WAIT_TIME = 2000;
//
//    private static final Map<String, String> EMPTY_MAP = Collections.emptyMap();
//
//    public LvmDriverTest() throws Exception
//    {
//        super(new LvmDriverKind());
//    }
//
//    @Test
//    public void testConfigVolumeGroup() throws StorageException
//    {
//        final HashMap<String, String> config = new HashMap<>();
//
//        String volumeGroup = "otherName";
//        config.put(CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup);
//        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup);
//        driver.setConfiguration(STOR_POOL_NAME, config, EMPTY_MAP, EMPTY_MAP);
//
//        ec.clearBehaviors();
//        volumeGroup = "_specialName";
//        config.put(CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup);
//        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup);
//        driver.setConfiguration(STOR_POOL_NAME, config, EMPTY_MAP, EMPTY_MAP);
//
//        ec.clearBehaviors();
//        volumeGroup = "special-Name";
//        config.put(CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup);
//        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup);
//        driver.setConfiguration(STOR_POOL_NAME, config, EMPTY_MAP, EMPTY_MAP);
//    }
//
//    @Test(expected = StorageException.class)
//    public void testConfigVolumeGroupValidNotExistent() throws StorageException
//    {
//        String volumeGroup = "valid";
//        Map<String, String> config = createMap(CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup);
//        expectCheckVolumeGroup(LVM_VGS_DEFAULT, volumeGroup, false);
//        driver.setConfiguration(STOR_POOL_NAME, config, EMPTY_MAP, EMPTY_MAP);
//    }
//
//    @Test(expected = StorageException.class)
//    public void testConfigVolumeGroupEmpty() throws StorageException
//    {
//        driver.setConfiguration(STOR_POOL_NAME, createMap(CONFIG_LVM_VOLUME_GROUP_KEY, ""), EMPTY_MAP, EMPTY_MAP);
//    }
//
//    @Test(expected = StorageException.class)
//    public void testConfigVolumeGroupWhitespacesOnly() throws StorageException
//    {
//        driver.setConfiguration(STOR_POOL_NAME, createMap(CONFIG_LVM_VOLUME_GROUP_KEY, "  "), EMPTY_MAP, EMPTY_MAP);
//    }
//
//    @Test
//    public void testConfigCommand() throws StorageException, IOException
//    {
//        expectException(createMap(CONFIG_LVM_CHANGE_COMMAND_KEY, "notLvmChange"));
//        expectException(createMap(CONFIG_LVM_CREATE_COMMAND_KEY, "notLvmCreate"));
//        expectException(createMap(CONFIG_LVM_LVS_COMMAND_KEY, "notLvs"));
//        expectException(createMap(CONFIG_LVM_REMOVE_COMMAND_KEY, "notLvmRemove"));
//        expectException(createMap(CONFIG_LVM_VGS_COMMAND_KEY, "notVgs"));
//
//        String vgsCommand = "otherVgs";
//        File tmpFile = tempFolder.newFile(vgsCommand);
//        tmpFile.setExecutable(true);
//        expectCheckVolumeGroup(tmpFile.getAbsolutePath(), LVM_VOLUME_GROUP_DEFAULT);
//        driver.setConfiguration(
//            STOR_POOL_NAME,
//            createMap(CONFIG_LVM_VGS_COMMAND_KEY, tmpFile.getAbsolutePath()),
//            EMPTY_MAP,
//            EMPTY_MAP
//        );
//
//        String volumeGroup = "newVolumeGroup";
//        expectCheckVolumeGroup(tmpFile.getAbsolutePath(), volumeGroup);
//        driver.setConfiguration(
//            STOR_POOL_NAME,
//            createMap(CONFIG_LVM_VOLUME_GROUP_KEY, volumeGroup),
//            EMPTY_MAP,
//            EMPTY_MAP
//        );
//    }
//
//    @Test
//    public void testConfigToleranceFactor() throws StorageException
//    {
//        expectCheckVolumeGroup(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT);
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
//        final String volumeIdentifier = "identifier";
//
//        final long maxtoleratedSize = TEST_SIZE_100MB + TEST_EXTENT_SIZE * TEST_TOLERANCE_FACTOR;
//        expectLvsInfoBehavior(
//            LVM_LVS_DEFAULT,
//            LVM_VOLUME_GROUP_DEFAULT,
//            volumeIdentifier,
//            Long.toString(maxtoleratedSize) + ".00"
//        );
//        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, "4096.00k");
//
//        {
//            StorageDriver.SizeComparison sizeComparison = driver.compareVolumeSize(
//                volumeIdentifier,
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
//        expectLvsInfoBehavior(
//            LVM_LVS_DEFAULT,
//            LVM_VOLUME_GROUP_DEFAULT,
//            volumeIdentifier,
//            Long.toString(maxtoleratedSize + 1) + ".00"
//        );
//
//        try
//        {
//            StorageDriver.SizeComparison sizeComparison = driver.compareVolumeSize(
//                volumeIdentifier,
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
//        final String identifier = "identifier";
//        driver.startVolume(identifier, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//    @Test
//    public void testStartUnknownVolume() throws StorageException
//    {
//        final String unknownIdentifier = "unknown";
//        driver.startVolume(unknownIdentifier, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//    @Test
//    public void testStopVolume() throws StorageException
//    {
//        final String identifier = "identifier";
//        driver.stopVolume(identifier, false, ReadOnlyProps.emptyRoProps()); // should not trigger anything
//    }
//
//    @Test
//    public void testStopUnknownVolume() throws StorageException
//    {
//        final String unknownIdentifier = "unknown";
//        driver.stopVolume(unknownIdentifier, false, ReadOnlyProps.emptyRoProps()); // should not trigger anything
//    }
//
//    @Test
//    public void testCreateVolumeDelayed() throws Exception
//    {
//        final long volumeSize = 102400;
//        final String identifier = "testVolume";
//        expectLvsInfoBehavior(
//            LVM_LVS_DEFAULT,
//            LVM_VOLUME_GROUP_DEFAULT,
//            identifier,
//            Long.toString(volumeSize) + ".00"
//        );
//        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, identifier, LVM_VOLUME_GROUP_DEFAULT, false);
//        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, TEST_EXTENT_SIZE);
//
//        final String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
//            expectedFilePath,
//            Event.CREATE,
//            testFileEntryGroup
//        );
//        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);
//
//        final FileEntry testEntry = new FileEntry(
//            Paths.get(expectedFilePath),
//            Event.CREATE,
//            emptyFileObserver);
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
//    public void testCreateVolumeInstant() throws Exception
//    {
//        final long volumeSize = 102400;
//        final String identifier = "testVolume";
//
//        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, identifier, LVM_VOLUME_GROUP_DEFAULT, false);
//        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, volumeSize);
//        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, TEST_EXTENT_SIZE);
//
//        final String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
//            expectedFilePath,
//            Event.CREATE,
//            testFileEntryGroup
//        );
//        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);
//
//        final FileEntry testEntry = new FileEntry(
//            Paths.get(expectedFilePath),
//            Event.CREATE,
//            emptyFileObserver);
//
//        testFileEntryGroup.fileEvent(testEntry);
//        driver.createVolume(identifier, volumeSize, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//    @Test(expected = StorageException.class)
//    public void testCreateVolumeTimeout() throws Exception
//    {
//        final long volumeSize = 102400;
//        final String identifier = "testVolume";
//
//        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, identifier, LVM_VOLUME_GROUP_DEFAULT, false);
//        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, TEST_EXTENT_SIZE);
//
//        final String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
//            expectedFilePath,
//            Event.CREATE,
//            testFileEntryGroup
//        );
//        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);
//
//        // do not fire file event
//        driver.createVolume(identifier, volumeSize, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//
//    @Test(expected = StorageException.class)
//    public void testCreateExistingVolume() throws Exception
//    {
//        final long volumeSize = 102400;
//        final String volumeName = "testVolume";
//
//        expectLvmCreateVolumeBehavior(LVM_CREATE_DEFAULT, volumeSize, volumeName, LVM_VOLUME_GROUP_DEFAULT, true);
//        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, TEST_EXTENT_SIZE);
//
//        driver.createVolume(volumeName, volumeSize, null, ReadOnlyProps.emptyRoProps()); // null == not encrypted
//    }
//
//    @Test
//    public void testDeleteVolume() throws Exception
//    {
//        final String identifier = "testVolume";
//        final int volumeSize = 102400;
//
//        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, volumeSize);
//        expectLvmDeleteVolumeBehavior(LVM_REMOVE_DEFAULT, identifier, LVM_VOLUME_GROUP_DEFAULT, true);
//
//        final String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
//            expectedFilePath,
//            Event.DELETE,
//            testFileEntryGroup
//        );
//        PowerMockito.whenNew(FileEntryGroupBuilder.class).withNoArguments().thenReturn(builderMock);
//
//        final FileEntry testEntry = new FileEntry(
//            Paths.get(expectedFilePath),
//            Event.DELETE,
//            emptyFileObserver);
//
//        testFileEntryGroup.fileEvent(testEntry);
//        driver.deleteVolume(identifier, false, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test(expected = StorageException.class)
//    public void testDeleteVolumeTimeout() throws Exception
//    {
//        final String identifier = "testVolume";
//        final int volumeSize = 102400;
//
//        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, volumeSize);
//        expectLvmDeleteVolumeBehavior(LVM_REMOVE_DEFAULT, identifier, LVM_VOLUME_GROUP_DEFAULT, true);
//
//        final String expectedFilePath = ((AbsStorageDriver) driver).getExpectedVolumePath(identifier);
//        final FileEntryGroup testFileEntryGroup = getInstance(FileEntryGroup.class);
//
//        final FileEntryGroupBuilder builderMock = getTestFileEntryGroupBuilder(
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
//        final String volumeName = "testVolume";
//
//        expectLvmDeleteVolumeBehavior(LVM_REMOVE_DEFAULT, volumeName, LVM_VOLUME_GROUP_DEFAULT, false);
//        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, volumeName, 0);
//
//        driver.deleteVolume(volumeName, false, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test
//    public void testCheckVolume() throws StorageException
//    {
//        final String identifier = "testVolume";
//        final long size = 102_400;
//
//        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, size);
//        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, TEST_EXTENT_SIZE);
//
//        driver.compareVolumeSize(identifier, size, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test(expected = StorageException.class)
//    public void testCheckVolumeSizeTooLarge() throws StorageException
//    {
//        final String identifier = "testVolume";
//        final long size = MetaData.DRBD_MAX_kiB + 1;
//
//        driver.compareVolumeSize(identifier, size, ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test
//    @SuppressWarnings("checkstyle:magicnumber")
//    public void testCheckVolumeTooSmall() throws StorageException
//    {
//        final String identifier = "testVolume";
//        final long size = 102_400;
//
//        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, "4096.00k");
//        expectLvsInfoBehavior(
//            LVM_LVS_DEFAULT,
//            LVM_VOLUME_GROUP_DEFAULT,
//            identifier,
//            size - 10); // user wanted at least 100 MB, but we give him a little bit less
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
//        final String identifier = "testVolume";
//
//        final String path = driver.getVolumePath(identifier, false, ReadOnlyProps.emptyRoProps());
//        assertEquals("/dev/" +
//            LVM_VOLUME_GROUP_DEFAULT + "/" +
//            identifier,
//            path);
//    }
//
//    @Test
//    public void testSize() throws StorageException
//    {
//        final String identifier = "testVolume";
//        final long volumeSize = 102_400;
//
//        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, volumeSize);
//
//        final long size = driver.getSize(identifier, ReadOnlyProps.emptyRoProps());
//        assertEquals(volumeSize, size);
//    }
//
//    @Test
//    public void testTotalSpace() throws StorageException
//    {
//        final long size = 1L * 1024 * 1024; // 1Gib
//        expectVgsTotalSpaceCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, size);
//
//        assertEquals(size, driver.getTotalSpace());
//    }
//
//    @Test
//    public void testFreeSpace() throws StorageException
//    {
//        final long size = 1L * 1024 * 1024 * 1024;
//        expectVgsFreeSpaceCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, size);
//
//        assertEquals(size, driver.getFreeSpace());
//    }
//
//    @Test(expected = StorageException.class)
//    public void testSizeUnknownVolume() throws StorageException
//    {
//        final String identifier = "testVolume";
//        final long volumeSize = 102_400;
//
//        expectLvsInfoBehavior(LVM_LVS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, identifier, volumeSize);
//
//        driver.getSize("otherVolume", ReadOnlyProps.emptyRoProps());
//    }
//
//    @Test
//    public void testTraits() throws StorageException
//    {
//        expectVgsExtentCommand(LVM_VGS_DEFAULT, LVM_VOLUME_GROUP_DEFAULT, TEST_EXTENT_SIZE);
//        Map<String, String> traits = driver.getTraits("unused");
//
//        final String size = traits.get(ApiConsts.KEY_STOR_POOL_ALLOCATION_UNIT);
//        assertEquals("4096", size);
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
//        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_CREATE_COMMAND_KEY));
//        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_RESIZE_COMMAND_KEY));
//        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_REMOVE_COMMAND_KEY));
//        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_CHANGE_COMMAND_KEY));
//        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY));
//        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY));
//        assertTrue(keys.remove(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY));
//        assertTrue(keys.remove(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY));
//
//        assertTrue(keys.isEmpty());
//    }
//}
