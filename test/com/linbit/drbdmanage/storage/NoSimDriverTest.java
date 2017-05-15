package com.linbit.drbdmanage.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbdmanage.ErrorReporter;
import com.linbit.drbdmanage.SatelliteCoreServices;
import com.linbit.drbdmanage.debug.DebugErrorReporter;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;
import com.linbit.timer.Timer;

/**
 * Although this class is in the test folder, this class is
 *
 * NOT A JUNIT TEST.
 *
 * The {@link LvmDriverTest}, {@link LvmThinDriverTest} and {@link ZfsDriverTest}
 * are JUnit tests, which simulates every external IO. However, this class will
 * not simulate anything, thus really executing all commands on the underlying system.
 * Because of this, we need to ensure the order of the "tests", which
 * is not possible with JUnit.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public abstract class NoSimDriverTest
{
    protected final String baseIdentifier = "drbdManageDriverTest-REMOVE-ME-Vol-";

    protected boolean poolExisted;
    protected String poolName;

    protected boolean log = true;

    protected StorageDriver driver;
    protected ExtCmd extCommand;

    protected boolean noCleanUp;
    protected boolean inCleanUp = false;

    private final Path baseMountPath = Paths.get("/mnt","drbdmanageNGtests");

    private boolean baseMountPathExisted;

    public NoSimDriverTest(StorageDriver driver) throws IOException, StorageException
    {
        this.driver = driver;
        SatelliteCoreServices coreSvc = new TestCoreServices();
        extCommand = new ExtCmd(coreSvc.getTimer());
        driver.initialize(coreSvc);
    }

    protected void runTest() throws StorageException, MaxSizeException, MinSizeException, ChildProcessTimeoutException, IOException, InterruptedException
    {
        initialize();

        String identifier = getUnusedIdentifier();

        log("testing createVolume   %n");

        long size = 100 * 1024;
        createVolume(identifier, size, "   creating volume [%s] with size [%d]... ");

        try
        {
            createVolume(identifier, size, "   trying to create existing volume [%s]...");
            fail("      Creating an existing volume [%s] should have thrown an exception", identifier);
        }
        catch (StorageException expected)
        {
            log(" done%n");
        }

        String identifier2 = getUnusedIdentifier();
        try
        {
            createVolume(identifier2, 10 * getPoolSizeInKiB(), "   trying to create too large volume [%s] with size [%d]...");
            if (!isThinDriver())
            {
                fail("      Creating too large volume should have failed");
            }
        }
        catch (StorageException storExc)
        {
            if (isThinDriver())
            {
                fail("      Unexpected execption (thin driver should be able to create this volume)");
            }
            else
            {
                log(" done%n");
            }
        }

        // if the size is too close to zero (e.g. -1) it might get aligned to a positive value...
        // -4096 is (currently) safe for both, lvm and zfs (safe by the means of this test)
        String invalidIdentifier = getUnusedIdentifier();
        try
        {
            int negativeSize = -4096;
            createVolume(invalidIdentifier, negativeSize, "   trying to create volume [%s] with negative size [%d]...");
            fail("      Creating volume with negative size should have thrown an exception");
        }
        catch (StorageException | MinSizeException expected)
        {
            log(" done%n");
        }

        String identifier3 = getUnusedIdentifier();
        createVolume(identifier3, MetaData.DRBD_MIN_NET_kiB - 1, "   trying to create volume [%s] with too small size [%d] (should be aligned up)...");


        log("testing checkVolume   %n");

        checkVolume(identifier, size, "   checking volume [%s], with size [%d]...");
        checkVolume(identifier, size - 1, "   checking volume [%s], with size [%d] (should be tolerated)...");
        try
        {
            checkVolume(identifier, size / 2, "   test checking volume [%s], with size [%d] (too low)...");
            fail("      checkVolume should have thrown StorageException");
        }
        catch (StorageException expected)
        {
            // expected
            log(" done%n");
        }
        try
        {
            checkVolume(identifier, size + 1, "   test checking volume [%s], with size [%d] (too high)...");
            fail("      checkVolume should have thrown StorageException");
        }
        catch (StorageException expected)
        {
            // expected
            log(" done%n");
        }
        try
        {
            checkVolume(identifier, -size, "   test checking volume [%s], with negative size [%d]...");
            fail("      checkVolume should have thrown StorageException");
        }
        catch (StorageException expected)
        {
            // expected
            log(" done%n");
        }

        String identifier4 = getUnusedIdentifier();
        long unalignedSize = 100 * 1024 - 1; // should be aligned up to 100 * 1024, aka [size]
        createVolume(identifier4, unalignedSize, "   creating volume [%s] with unaligned size [%d]...");
        checkVolume(identifier4, unalignedSize, "   checking volume [%s], with unaligned size [%d]...");
        checkVolume(identifier4, size, "   checking volume [%s], with aligned size [%d]...");

        log("testing getSize   %n");
        checkSize(identifier, size, "   getting size of volume [%s]...");

        log("testing getPath   %n");
        checkPath(identifier, "   getting path of volume [%s]...");

        if (isVolumeStartStopSupportedImpl())
        {
            log("testing start and stop volume   %n");
            stopVolume(identifier, "   stopping volume [%s]...");
            startVolume(identifier, "   starting volume [%s]...");
        }

        if (driver.isSnapshotSupported())
        {
            log("testing snapshot create   %n");

            log("   checking if [%s] exists...", baseMountPath.toString());
            baseMountPathExisted = true;
            if (!Files.exists(baseMountPath))
            {
                log(" no - creating [%s]...", baseMountPath.toString());
                Files.createDirectories(baseMountPath);
                log(" done %n");
                baseMountPathExisted = false;
            }
            else
            {
                log(" yes%n");
                baseMountPathExisted = true;
            }

            String origMountPath = getUnusedMountPath(identifier);
            String origBlockDevice = getVolumePathImpl(identifier);
            String testFile1 = "test1";
            String testFile2 = "test2";
            String testFile3 = "test3";
            String testFile4 = "test4";

            String origTestFile1 = origMountPath + File.separator + testFile1;
            String origTestFile2 = origMountPath + File.separator + testFile2;
            String origTestFile3 = origMountPath + File.separator + testFile3;

            createMountPoint(origMountPath, "   creating mountponit [%s]...");
            createExt4(origBlockDevice, "   creating ext4 on device [%s]...");
            mount(origBlockDevice, origMountPath, "   mounting [%s] to mountpoint [%s]...");

            log("   creating test data in [%s]...", origMountPath);
            callChecked("touch", origTestFile1);
            callChecked("/bin/sh", "-c","echo 'test' > " + origTestFile2);
            // file status:
            // test1 - touched
            // test2 - "test"
            // test3 - not created
            // test4 - not created
            log(" done%n");

            unmount(origMountPath, "   unmounting (force flush) [%s]...");

            String snapName1 = getUnusedIdentifier("", "_snap");
            createSnapshot(identifier, snapName1, "   creating snapshot [%s] from volume [%s]...");

            mount(origBlockDevice, origMountPath, "   remounting [%s] to mountpoint [%s]...");

            String snap1BlockDevice = getVolumePathImpl(snapName1);
            String snap1MountPath = getUnusedMountPath(snapName1);
            String snap1TestFile1 = snap1MountPath + File.separator + testFile1;
            String snap1TestFile2 = snap1MountPath + File.separator + testFile2;
            String snap1TestFile3 = snap1MountPath + File.separator + testFile3;
            String snap1TestFile4 = snap1MountPath + File.separator + testFile4;

            createMountPoint(snap1MountPath, "   creating mountpoint [%s]...");
            mount(snap1BlockDevice, snap1MountPath, "   mounting [%s] to mountpoint [%s]...");

            log("   modifying test data...");
            callChecked("/bin/sh", "-c","echo 'overridden Test' > " + origTestFile2);
            callChecked("touch", origTestFile3);
            callChecked("rm", origTestFile1);
            // file status:
            // test1 - removed
            // test2 - "overridden Test"
            // test3 - touched
            // test4 - not created
            log(" done%n");

            log("   checking existence of files...");
            checkFileExists(true, snap1TestFile1, "\n      ", 10, 1000);
            checkFileExists(true, snap1TestFile2, "\n      ", 10, 1000);
            checkFileExists(false, snap1TestFile3, "\n      ", 10, 1000);
            checkFileExists(false, snap1TestFile4, "\n      ", 10, 1000);
            log (" done%n");

            log("   checking content of modified files...");
            OutputData catOut = callChecked("cat", snap1TestFile2);
            String cat = new String(catOut.stdoutData).trim();
            if (!cat.equals("test"))
            {
                log("   error: content of file [%s] %n      expected: [%s]%n      received: [%s]",
                    snap1TestFile2,
                    "test",
                    cat
                );
                fail("Snapshot contains modified content in file %s", testFile2);
            }
            log(" done%n");

            log("testing snapshot clone   %n");

            // file status:
            // test1 - removed
            // test2 - "overridden Test"
            // test3 - touched
            // test4 - not created

            unmount(snap1MountPath, "   unmounting (force flush) [%s]...");

            String snapName2 = getUnusedIdentifier("", "_snap2");
            cloneSnapshot(snapName1, snapName2, "   cloning snapshot [%s] from [%s]...");

            mount(snap1BlockDevice, snap1MountPath, "   remounting snapshot from [%s] to [%s]...");

            log("   modifying data of the first snapshot [%s]...", snapName1);
            callChecked("/bin/sh", "-c","echo 'another overridden Test' > " + origTestFile2);
            callChecked("touch", snap1TestFile4);
            callChecked("rm", snap1TestFile1);
            // file status:
            // test1 - removed
            // test2 - "another overridden Test"
            // test3 - removed
            // test4 - touched
            log(" done%n");

            String snap2MountPath = getUnusedMountPath(snapName2);
            String snap2BlockDevice = getVolumePathImpl(snapName2);
            String snap2TestFile1 = snap2MountPath + File.separator + testFile1;
            String snap2TestFile2 = snap2MountPath + File.separator + testFile2;
            String snap2TestFile3 = snap2MountPath + File.separator + testFile3;
            String snap2TestFile4 = snap2MountPath + File.separator + testFile4;

            createMountPoint(snap2MountPath, "   creating mountpoint [%s]...");
            mount(snap2BlockDevice, snap2MountPath,"   mounting [%s] to mountpoint [%s]...");

            log("   checking existence of files...");
            checkFileExists(true, snap2TestFile1, "\n      ", 10, 1000);
            checkFileExists(true, snap2TestFile2, "\n      ", 10, 1000);
            checkFileExists(false, snap2TestFile3, "\n      ", 10, 1000);
            checkFileExists(false, snap2TestFile4, "\n      ", 10, 1000);
            log (" done%n");

            log("   checking content of modified files...");
            catOut = callChecked("cat", snap2TestFile2);
            cat = new String(catOut.stdoutData).trim();
            if (!cat.equals("test"))
            {
                log("   error: content of file [%s] %n      expected: [%s]%n      received: [%s]",
                    snap2TestFile2,
                    "test",
                    cat
                );
                fail("Snapshot contains modified content in file %s", testFile2);
            }
            log(" done%n");


            // TODO test restore

            // restore snapshot may take very long (depending on the size of the modified data)
            // which could cause the extCmd to cause a timeout

            // driver.restoreSnapshot(snapName1);

            log("testing snapshot delete   %n");

            unmount(snap1MountPath, "   unmounting [%s]...");
            unmount(snap2MountPath, "   unmounting [%s]...");
            unmount(origMountPath, "   unmounting [%s]...");

            deleteSnapshot(snapName2, "   trying to delete second snapshot [%s]...");
            deleteSnapshot(snapName1, "   trying to delete first snapshot [%s]...");

            if (!baseMountPathExisted)
            {
                Files.delete(Paths.get(snap2MountPath));
                Files.delete(Paths.get(snap1MountPath));
                Files.delete(Paths.get(origMountPath));
                Files.delete(baseMountPath);
            }
        }

        log("testing deleteVolume   %n");

        deleteVolume(identifier, "   deleting volume [%s]...");

        try
        {
            deleteVolume(identifier, "   test deleting already deleted volume [%s]...");
            fail("      deleteVolume should have thrown StorageException");
        }
        catch(StorageException expected)
        {
            // expected
            log(" done%n");
        }

        try
        {
            deleteVolume("unknownVolume", "   test deleting unknown volume [%s]...");
            fail("      deleteVolume should have thrown StorageException");
        }
        catch(StorageException expected)
        {
            // expected
            log(" done%n");
        }

        if (isThinDriver())
        {
            deleteVolume(identifier2, "   deleting volume [%s]...");
        }
        deleteVolume(identifier3, "   deleting volume [%s]...");
        deleteVolume(identifier4, "   deleting volume [%s]...");

        log("%n%nAll tests done - no errors found%n");
        log("Running cleanup%n");
        cleanUp();
    }

    protected final void failIf(boolean condition, String format, Object...args) throws ChildProcessTimeoutException, IOException
    {
        if(condition)
        {
            fail(format, args);
        }
    }

    protected final void fail(String format, Object... args) throws ChildProcessTimeoutException, IOException
    {
        if (!inCleanUp) // prevent endless recursion
        {
            log("%n%n<<<FAILED [" + String.format(format, args).trim() + "]>>>%n");
            log("<<<RUNNING CLEANUP>>>%n%n%n");
            cleanUp();
        }
        throw new TestFailedException(String.format(format, args));
    }

    protected OutputData callChecked(String... command) throws ChildProcessTimeoutException, IOException
    {
        OutputData exec = extCommand.exec(command);
        if (exec.exitCode != 0)
        {
            StringBuilder sb = new StringBuilder();
            for (String cmd : command)
            {
                sb.append(cmd).append(" ");
            }
            System.err.println();
            System.err.println();
            System.err.println();
            System.err.println("Could not execute: " + sb.toString());
            System.err.println(new String(exec.stderrData));
            fail("Failed to execute command [%s]", sb.toString());
        }
        return exec;
    }

    protected void checkFileExists(boolean shouldExist, String file, String logIndent, int retries, int retryDelay) throws InterruptedException
    {
        try
        {
            int origRetryCount = retries;
            do
            {
                OutputData outData = extCommand.exec("ls", file);
                boolean lsSuccess = outData.exitCode == 0;
                if (lsSuccess)
                {
                    if (shouldExist)
                    {
                        break; // all good
                    }
                    else
                    {
                        fail("%sFile [%s] should not exist, but it does", logIndent, file);
                    }
                }
                else
                {
                    if (shouldExist)
                    {
                        if (retries > 0) // skip the last (thus unnecessary) sleep
                        {
                            log("%sFile [%s] does not exist (yet). Waiting %d ms (%d / %d)",
                                logIndent,
                                file,
                                retryDelay,
                                origRetryCount - retries + 1,
                                origRetryCount
                            );
                            Thread.sleep(retryDelay);
                        }
                    }
                    else
                    {
                        break; // all good
                    }
                }
            }
            while (retries --> 0);
            if (retries <= 0)
            {
                fail("%sFile [%s] should exist, but it does not.", logIndent, file, origRetryCount, retryDelay);
            }
        }
        catch (ChildProcessTimeoutException | IOException e)
        {
            e.printStackTrace();
        };
    }

    protected void log(String format, Object...args)
    {
        if (log)
        {
            System.out.format(format, args);
        }
    }

    private void initialize() throws ChildProcessTimeoutException, IOException, StorageException
    {
        log("initializing... %n");
        initializeImpl();
        setPoolName(poolName);
        log("initialized %n");
    }

    private String getUnusedIdentifier() throws ChildProcessTimeoutException, IOException
    {
        return getUnusedIdentifier("", "");
    }

    private String getUnusedIdentifier(String prefix, String suffix) throws ChildProcessTimeoutException, IOException
    {
        log("   requesting unused identifier... ");

        String identifier = null;
        int i = 0;
        while (identifier == null)
        {
            identifier = baseIdentifier + i;
            i++;
            if (volumeExists(identifier))
            {
                identifier = null;
            }
        }
        identifier = prefix + identifier + suffix;
        log("using identifier: [%s]... done %n", identifier);
        return identifier;
    }

    private void createVolume(String identifier, long size, String format) throws MaxSizeException, MinSizeException, StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier, size);
        driver.createVolume(identifier, size);
        failIf(!volumeExists(identifier), "Failed to create volume [%s]", identifier);
        log(" done %n");
    }

    private void checkVolume(String identifier, long size, String format) throws StorageException
    {
        log(format, identifier, size);
        driver.checkVolume(identifier, size);
        log(" done %n");
    }

    private void checkSize(String identifier, long expectedSize, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        long driverSize = driver.getSize(identifier);
        failIf(driverSize != expectedSize, "expected size [%d] but was [%d]", expectedSize, driverSize);
        log(" done %n");
    }

    private void checkPath(String identifier, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        String volumePath = driver.getVolumePath(identifier);
        String expectedVolumePath = getVolumePathImpl(identifier);
        failIf(!expectedVolumePath.equals(volumePath), "unexpected volume path: [%s], expected: [%s]", volumePath, expectedVolumePath);
        log(" done %n");
    }

    private void stopVolume(String identifier, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        driver.stopVolume(identifier);
        failIf(isVolumeStartedImpl(identifier), "volume [%s] failed to stop", identifier);
        log(" done %n");
    }

    private void startVolume(String identifier, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        driver.startVolume(identifier);
        failIf(!isVolumeStartedImpl(identifier), "volume [%s] failed to start", identifier);
        log(" done %n");
    }

    private void deleteVolume(String identifier, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        driver.deleteVolume(identifier);
        failIf(volumeExists(identifier), "Failed to delete volume [%s]", identifier);
        log(" done %n");
    }

    private String getUnusedMountPath(String identifier)
    {
        int count = -1;
        Path path;
        do
        {
            String id;
            if (count == -1)
            {
                id = identifier;
            }
            else
            {
                id = identifier + "_" + count;
            }
            path = baseMountPath.resolve(id);
            count++;
        }
        while (Files.exists(path));
        String mountPath = path.toAbsolutePath().toString();
        return mountPath;
    }

    private void unmount(String mountPoint, String format) throws ChildProcessTimeoutException, IOException
    {
        log(format, mountPoint);
        callChecked("umount", mountPoint);
        log(" done %n");
    }

    private void deleteSnapshot(String identifier, String format) throws StorageException, ChildProcessTimeoutException, IOException
    {
        log(format, identifier);
        driver.deleteSnapshot(identifier);
        failIf(volumeExists(identifier), "Failed to delete snapshot [%s]", identifier);
        log (" done%n");
    }

    private void createMountPoint(String mountPath, String format) throws ChildProcessTimeoutException, IOException
    {
        log(format, mountPath);
        callChecked("mkdir", mountPath);
        log(" done %n");
    }

    private void createExt4(String blockDevice, String format) throws ChildProcessTimeoutException, IOException
    {
        log(format, blockDevice);
        callChecked("mkfs", "-t", "ext4", blockDevice);
        log(" done%n");
    }

    private void mount(String blockDevice, String mountPath, String format) throws ChildProcessTimeoutException, IOException
    {
        log(format, blockDevice, mountPath);
        callChecked("mount", blockDevice, mountPath);
        log(" done%n");
    }

    private void createSnapshot(String identifier, String snapshotName, String format) throws StorageException
    {
        log(format, snapshotName, identifier);
        driver.createSnapshot(identifier, snapshotName);
        log(" done%n");
    }

    private void cloneSnapshot(String snapshotName1, String snapshotName2, String format) throws StorageException
    {
        log(format, snapshotName1, snapshotName2);
        driver.cloneSnapshot(snapshotName1, snapshotName2);
        log(" done%n");
    }

    protected void cleanUp() throws ChildProcessTimeoutException, IOException
    {
        if (!inCleanUp)
        {
            inCleanUp = true;
            log("cleaning up...%n");
            if (noCleanUp)
            {
                log("   noCleanUp flag is set - skipping cleanup  %n");
            }
            else
            {
                if (Files.exists(baseMountPath))
                {
                    log("   checking if devices are still mounted...%n");
                    OutputData mountOutput = callChecked("mount");
                    String std = new String(mountOutput.stdoutData);
                    String[] lines = std.split("\n");
                    String baseMountPath = this.baseMountPath.toAbsolutePath().toString();
                    for (String line : lines)
                    {
                        String[] mountInfo = line.split(" ");
                        String mountPoint = mountInfo[2];
                        if (mountPoint.startsWith(baseMountPath))
                        {
                            log("      unmounting %s %n", mountPoint);
                            callChecked("umount", mountPoint);
                        }
                    }
                    log("         done%n");
                    // all mounts should have been unmounted

                    String baseMountString = baseMountPath.toString();
                    // just to be sure that we are not executing "rm -rf /" or /*
                    if (baseMountString.startsWith("/mnt/"))
                    {
                        if (baseMountPathExisted)
                        {
                            baseMountString += "/*";
                        }
                        callChecked("rm", "-rf", baseMountString);
                    }


                }
                cleanUpDriver();

                log("%nall should be cleared now %n%n%n");
            }
        }
    }

    protected void cleanUpDriver() throws ChildProcessTimeoutException, IOException
    {
        cleanUpVolumes();
        cleanUpPool();
    }

    protected void cleanUpVolumes() throws ChildProcessTimeoutException, IOException
    {
        log("   checking test volume(s)... %n");
        String[] listNamesCommand = getListVolumeNamesCommand();
        OutputData listNames = callChecked(listNamesCommand);
        String[] identifiers = new String(listNames.stdoutData).split("\n");
        boolean volumeFound = false;
        int volumesRemoved;
        int volumesFailedToRemove;
        do
        {
            volumesRemoved = 0;
            volumesFailedToRemove = 0;
            for (String identifier : identifiers)
            {
                identifier = identifier.trim();
                if (identifier.startsWith(baseIdentifier))
                {
                    volumeFound = true;
                    log("      found volume [%s], trying to remove...", identifier);
                    removeVolume(identifier);
//                    callChecked("lvremove", "-f", poolName + File.separator + identifier);
                    log(" done %n");

                    log("         verifying remove...");
                    listNames = callChecked(listNamesCommand);
                    identifiers = new String(listNames.stdoutData).split("\n");
                    for (String id : identifiers)
                    {
                        if (id.equals(identifier))
                        {
    //                        fail("Failed to remove test volume [%s] in cleanup", identifier);
                            log("         Failed to remove test volume [%s] - maybe because of dependencies - will retry",
                                identifier
                            );
                            volumesFailedToRemove++;
                        }
                    }
                    volumesRemoved++;
                    log(" done%n");
                }
            }
        }
        while (volumesFailedToRemove > 0 && volumesRemoved > 0);
        if (!volumeFound)
        {
            log("      no volumes found %n");
        }
        else
        if (volumesFailedToRemove > 0)
        {
            fail("Failed to remove test volumes: %n%s", new String(listNames.stdoutData));
        }
    }

    protected void cleanUpPool() throws ChildProcessTimeoutException, IOException
    {
        if (!poolExisted)
        {
            if (poolExists())
            {
                log("   pool did not exist before tests - trying to remove...");
                removePool();
                log(" done %n");

                log("      verifying remove...");
                OutputData vgs = callChecked(getListPoolNamesCommand());
                String[] lines = new String(vgs.stdoutData).split("\n");
                for (String line : lines)
                {
                    if (poolName.equals(line.trim()))
                    {
                        fail("Failed to remove test volume group(s) [%s] in cleanup", poolName);
                    }
                }
                log(" done%n");
            }
            else
            {
                log("   pool did not exist before tests - but it does not exist now either. nothing to do %n");
            }
        }
    }

    protected void setPoolName(String poolName) throws StorageException
    {
        Map<String, String> config = getPoolConfigMap(poolName);
        driver.setConfiguration(config);
    }

    protected Map<String, String> getPoolConfigMap(String poolName)
    {
        Map<String, String> config = new HashMap<>();
        config.put(StorageConstants.CONFIG_ZFS_POOL_KEY, poolName);
        config.put(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY, poolName);
        return config;
    }

    protected void main0(String...args) throws ChildProcessTimeoutException, IOException
    {
        boolean cleanUpOnly = args.length >= 1 && args[0].equalsIgnoreCase("cleanup");
        noCleanUp = args.length >= 1 && args[0].equalsIgnoreCase("nocleanup");
        try
        {
            if (cleanUpOnly)
            {
                cleanUp();
            }
            else
            {
                this.runTest();
                cleanUp();
            }
        }
        catch (Exception e)
        {
            System.err.println();
            e.printStackTrace();
            cleanUp();
        }
    }

    protected abstract void initializeImpl() throws ChildProcessTimeoutException, IOException;

    protected abstract boolean isThinDriver();

    protected abstract long getPoolSizeInKiB() throws ChildProcessTimeoutException, IOException;

    protected abstract boolean volumeExists(String identifier) throws ChildProcessTimeoutException, IOException;

    protected abstract String getVolumePathImpl(String identifier) throws ChildProcessTimeoutException, IOException;

    protected abstract boolean isVolumeStartStopSupportedImpl();

    protected abstract boolean isVolumeStartedImpl(String identifier);

    protected abstract String[] getListVolumeNamesCommand();

    protected abstract void removeVolume(String identifier) throws ChildProcessTimeoutException, IOException;

    protected abstract boolean poolExists() throws ChildProcessTimeoutException, IOException;

    protected abstract void removePool() throws ChildProcessTimeoutException, IOException;

    protected abstract String[] getListPoolNamesCommand();

    private static class TestCoreServices implements SatelliteCoreServices
    {
        private final GenericTimer<String, Action<String>> timerEventSvc ;
        private final FileSystemWatch fsEventSvc;
        private DebugErrorReporter errorReporter;

        public TestCoreServices() throws IOException
        {
            timerEventSvc = new GenericTimer<>();
            fsEventSvc = new FileSystemWatch();
            errorReporter = new DebugErrorReporter(System.err);
        }

        @Override
        public ErrorReporter getErrorReporter()
        {
            return errorReporter;
        }

        @Override
        public Timer<String, Action<String>> getTimer()
        {
            return timerEventSvc;
        }

        @Override
        public FileSystemWatch getFsWatch()
        {
            return fsEventSvc;
        }
    }

    protected static class TestFailedException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        public TestFailedException()
        {
            super();
        }

        public TestFailedException(String message, Throwable cause)
        {
            super(message.trim(), cause);
        }

        public TestFailedException(String message)
        {
            super(message.trim());
        }

        public TestFailedException(Throwable cause)
        {
            super(cause);
        }
    }

}
