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
// * The {@link LvmDriverTest} is a JUnit test, which simulates every
// * external IO. However, this class will not simulate anything, thus
// * really executing all commands on the underlying system.
// * Because of this, we need to ensure the order of the "tests", which
// * is not possible with JUnit.
// *
// * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
// */
//public class NoSimLvmDriverTest extends NoSimDriverTest
//{
//    public NoSimLvmDriverTest() throws IOException
//    {
//        this(new LvmDriverKind());
//    }
//
//    public NoSimLvmDriverTest(StorageDriverKind driverKind) throws IOException
//    {
//        super(driverKind);
//        poolExisted = false;
//        poolName = "linStorLvmDriverTestPool-REMOVE-ME";
//    }
//
//
//    public static void main(String[] args) throws IOException, StorageException, ChildProcessTimeoutException
//    {
//        NoSimLvmDriverTest driverTest = new NoSimLvmDriverTest();
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
//            String blockDevice = "/dev/loop1";
//            log("      creating pool [%s] on [%s]...", poolName, blockDevice);
//            callChecked("vgcreate", poolName, blockDevice);
//            log(" done %n");
//        }
//    }
//
//    @Override
//    protected boolean isThinDriver()
//    {
//        return false;
//    }
//
//    @Override
//    protected long getPoolSizeInKiB() throws ChildProcessTimeoutException, IOException
//    {
//        OutputData vgsOut = callChecked("vgs", "-o", "vg_size", "--noheading", "--units", "k");
//        String stringSize = new String(vgsOut.stdoutData).trim();
//        return Long.parseLong(stringSize.split("[,.]")[0]);
//    }
//
//    @Override
//    protected boolean volumeExists(String identifier, String snapName) throws ChildProcessTimeoutException, IOException
//    {
//        boolean exists = false;
//        OutputData lvs = callChecked("lvs", "-o", "lv_name", "--noheading");
//        String[] lines = new String(lvs.stdoutData).split("\n");
//
//        String targetId = identifier;
//        if (snapName != null)
//        {
//            targetId += "_" + snapName;
//        }
//
//        for (String line : lines)
//        {
//            if (targetId.equals(line.trim()))
//            {
//                exists = true;
//                break;
//            }
//        }
//        return exists;
//    }
//
//    @Override
//    protected String getVolumePathImpl(String identifier) throws ChildProcessTimeoutException, IOException
//    {
//        OutputData lvs = callChecked("lvs", "-o", "lv_name,lv_path", "--separator", ",", "--noheading");
//        String[] lines = new String(lvs.stdoutData).split("\n");
//        String path = null;
//        for (String line : lines)
//        {
//            String[] colums = line.trim().split(",");
//            if (identifier.equals(colums[0]))
//            {
//                path = colums[1];
//                break;
//            }
//        }
//        return path;
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
//        return true; // LvmDriver cannot stop or start
//    }
//
//    @Override
//    protected String[] getListVolumeNamesCommand()
//    {
//        return new String[] {"lvs", "-o", "lv_name", "--noheading"};
//    }
//
//    @Override
//    protected void removeVolume(String identifier) throws ChildProcessTimeoutException, IOException
//    {
//        callChecked("lvremove", "-f", poolName + File.separator + identifier);
//    }
//
//    @Override
//    protected boolean poolExists() throws ChildProcessTimeoutException, IOException
//    {
//        boolean ret = false;
//        OutputData outputData = callChecked("vgs", "-o", "vg_name", "--noheading");
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
//        callChecked("vgremove", poolName);
//    }
//
//    @Override
//    protected String[] getListPoolNamesCommand()
//    {
//        return new String[] {"vgs", "-o", "vg_name", "--noheading"};
//    }
//}
