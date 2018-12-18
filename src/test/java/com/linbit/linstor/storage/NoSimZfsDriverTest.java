package com.linbit.linstor.storage;

//import java.io.File;
//import java.io.IOException;
//
//import com.linbit.ChildProcessTimeoutException;
//import com.linbit.extproc.ExtCmd.OutputData;
//
///**
// * Although this class is in the test folder, this class is
// *
// * NOT A JUNIT TEST.
// *
// * The {@link ZfsDriverTest} is a JUnit test, which simulates every
// * external IO. However, this class will not simulate anything, thus
// * really executing all commands on the underlying system.
// * Because of this, we need to ensure the order of the "tests", which
// * is not possible with JUnit.
// *
// * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
// */
//public class NoSimZfsDriverTest extends NoSimDriverTest
//{
//    public NoSimZfsDriverTest() throws IOException
//    {
//        this(new ZfsDriverKind());
//    }
//
//    public NoSimZfsDriverTest(ZfsDriverKind driverKind) throws IOException
//    {
//        super(driverKind);
//        poolName = "testPool";
//    }
//
//    public static void main(String[] args) throws IOException, StorageException, ChildProcessTimeoutException
//    {
//        NoSimZfsDriverTest driverTest = new NoSimZfsDriverTest();
//        driverTest.main0(args);
//    }
//
//    @Override
//    protected void initializeImpl() throws ChildProcessTimeoutException, IOException
//    {
//        // ensure that the default pool exists
//        log("   checking if pool [%s] exists...", poolName);
//        poolExisted = poolExists();
//        if (poolExisted)
//        {
//            log(" yes %n");
//        }
//        else
//        {
//            log(" no %n");
//            String blockDevice = "/dev/loop0";
//            log("   creating pool [%s] on [%s]...", poolName, blockDevice);
//            callChecked("zpool", "create", poolName, blockDevice);
//            log(" done %n");
//        }
//    }
//
//    @Override
//    @SuppressWarnings("checkstyle:magicnumber")
//    protected long getPoolSizeInKiB() throws ChildProcessTimeoutException, IOException
//    {
//        OutputData vgsOut = callChecked("zpool", "get", "size", "-Hp", poolName);
//        String stringSize = new String(vgsOut.stdoutData).trim();
//        String[] lines = stringSize.split("\n");
//
//        Long ret = null;
//
//        for (String line : lines)
//        {
//            String[] columns = line.split("\t"); // forced by -p (parsable) option
//            if (columns[0].equals(poolName))
//            {
//                ret = Long.parseLong(columns[2]) >> 10; // convert to kiB
//                break;
//            }
//        }
//        if (ret == null)
//        {
//            throw new RuntimeException("Could not find zpool: " + poolName);
//        }
//        return ret;
//    }
//
//    @Override
//    protected boolean isThinDriver()
//    {
//        return false;
//    }
//
//    @Override
//    protected boolean volumeExists(String identifier, String snapName) throws ChildProcessTimeoutException, IOException
//    {
//        OutputData zfsList = callChecked("zfs", "list", "-o", "name", "-H");
//        String zfsOut = new String(zfsList.stdoutData);
//        String[] lines = zfsOut.split("\n");
//
//        String targetId = poolName + File.separator + identifier;
//        if (snapName != null)
//        {
//            targetId += "@" + snapName;
//        }
//
//        boolean exists = false;
//        for (String line : lines)
//        {
//            if (line.equals(targetId))
//            {
//                exists = true;
//                break;
//            }
//        }
//
//        return exists;
//    }
//
//    @Override
//    protected String getVolumePathImpl(String identifier) throws ChildProcessTimeoutException, IOException
//    {
//        return "/dev" +
//            File.separator + "zvol" +
//            File.separator + poolName +
//            File.separator + identifier;
//    }
//
//    @Override
//    protected boolean isVolumeStartStopSupportedImpl()
//    {
//        return false;
//    }
//
//    @Override
//    protected boolean isVolumeStartedImpl(String identifier)
//    {
//        return false; // will never get called
//    }
//
//    @Override
//    protected String[] getListVolumeNamesCommand()
//    {
//        return new String[] {"zfs", "list", "-o", "name", "-H"};
//    }
//
//    @Override
//    protected void removeVolume(String identifier) throws ChildProcessTimeoutException, IOException
//    {
//        callChecked("zfs", "destroy", poolName + File.separator + identifier);
//    }
//
//    @Override
//    protected String[] getListPoolNamesCommand()
//    {
//        return new String[] {"zpool", "list", "-o", "name", "-H"};
//    }
//
//    @Override
//    protected boolean poolExists() throws ChildProcessTimeoutException, IOException
//    {
//        boolean ret = false;
//        OutputData outputData = callChecked("zfs", "list", "-o", "name", "-H");
//
//        String[] lines = new String(outputData.stdoutData).split("\n");
//        for (String line : lines)
//        {
//            if (line.trim().equals(poolName))
//            {
//                ret = true;
//                break;
//            }
//        }
//        return ret;
//    }
//
//    @Override
//    protected void removePool() throws ChildProcessTimeoutException, IOException
//    {
//        callChecked("zpool", "destroy", poolName);
//    }
//}
