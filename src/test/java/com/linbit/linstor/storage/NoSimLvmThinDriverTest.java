package com.linbit.linstor.storage;

//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//
//import com.linbit.ChildProcessTimeoutException;
//import com.linbit.extproc.ExtCmd.OutputData;
//
///**
// * Although this class is in the test folder, this class is
// *
// * NOT A JUNIT TEST.
// *
// * The {@link LvmThinDriverTest} is a JUnit test, which simulates every
// * external IO. However, this class will not simulate anything, thus
// * really executing all commands on the underlying system.
// * Because of this, we need to ensure the order of the "tests", which
// * is not possible with JUnit.
// *
// * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
// */
//public class NoSimLvmThinDriverTest extends NoSimLvmDriverTest
//{
//    protected boolean thinPoolExisted = false;
//    protected String thinPool = LvmThinDriver.LVM_THIN_POOL_DEFAULT;
//
//    public NoSimLvmThinDriverTest() throws IOException
//    {
//        super(new LvmThinDriverKind());
//    }
//
//    public NoSimLvmThinDriverTest(StorageDriverKind driverKind) throws IOException
//    {
//        super(driverKind);
//    }
//
//    public static void main(String[] args) throws IOException, StorageException, ChildProcessTimeoutException
//    {
//        NoSimLvmThinDriverTest driverTest = new NoSimLvmThinDriverTest();
//        driverTest.main0(args);
//    }
//
//    @Override
//    protected void initializeImpl() throws ChildProcessTimeoutException, IOException
//    {
//        super.initializeImpl();
//        log("   checking if thinpool [%s] exists...", thinPool);
//        if (volumeExists(thinPool))
//        {
//            thinPoolExisted = true;
//            log(" yes %n");
//        }
//        else
//        {
//            thinPoolExisted = false;
//            log(" no %n");
//            log("      creating thinpool [%s]...", thinPool);
//            callChecked("lvcreate", "--size", "100M", "-T", poolName + File.separator + thinPool);
//            log(" done%n");
//        }
//    }
//
//    @Override
//    protected boolean isThinDriver()
//    {
//        return true;
//    }
//
//    @Override
//    protected boolean isVolumeStartStopSupportedImpl()
//    {
//        return true;
//    }
//
//    @Override
//    protected boolean isVolumeStartedImpl(String identifier)
//    {
//        return Files.exists(Paths.get("/dev", poolName, identifier));
//    }
//
//    @Override
//    protected void cleanUpDriver() throws ChildProcessTimeoutException, IOException
//    {
//        super.cleanUpVolumes();
//        cleanThinPool();
//        super.cleanUpPool();
//    }
//
//    protected void cleanThinPool() throws ChildProcessTimeoutException, IOException
//    {
//        if (!thinPoolExisted)
//        {
//            if (volumeExists(thinPool))
//            {
//                log("   thinpool did not exist before tests - trying to remove...");
//                String poolId = poolName + File.separator + thinPool;
//                callChecked("lvremove", "-f", poolId);
//                log(" done %n");
//                log("      verifying remove...");
//                OutputData lvs = callChecked("lvs", "-o", "lv_name", "--noheading");
//                String[] lines = new String(lvs.stdoutData).split("\n");
//                for (String line : lines)
//                {
//                    if (poolId.equals(line.trim()))
//                    {
//                        fail("Failed to remove test thin pool [%s] in cleanup", thinPool);
//                    }
//                }
//                log(" done %n");
//            }
//            else
//            {
//                log("   thinpool did not exist before tests - but it does not exist now - nothing to remove%n");
//            }
//        }
//    }
//}
