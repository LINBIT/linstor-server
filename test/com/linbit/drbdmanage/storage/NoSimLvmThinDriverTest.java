package com.linbit.drbdmanage.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd.OutputData;

/**
 * Although this class is in the test folder, this class is
 *
 * NOT A JUNIT TEST.
 *
 * The {@link LvmThinDriverTest} is a JUnit test, which simulates every
 * external IO. However, this class will not simulate anything, thus
 * really executing all commands on the underlying system.
 * Because of this, we need to ensure the order of the "tests", which
 * is not possible with JUnit.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class NoSimLvmThinDriverTest extends NoSimLvmDriverTest
{
    protected boolean thinPoolExisted = false;
    protected String thinPool = LvmThinDriver.LVM_THIN_POOL_DEFAULT;

    public NoSimLvmThinDriverTest() throws IOException, StorageException
    {
        super(new LvmThinDriver());
    }

    public NoSimLvmThinDriverTest(StorageDriver driver) throws IOException, StorageException
    {
        super(driver);
    }

    public static void main(String[] args) throws IOException, StorageException, ChildProcessTimeoutException
    {
        NoSimLvmThinDriverTest driverTest = new NoSimLvmThinDriverTest();
        try
        {
            driverTest.runTest();
        }
        catch (Exception exc)
        {
            System.err.println();
            exc.printStackTrace();
        }
        driverTest.cleanUpImpl();
    }

    @Override
    protected void initializeImpl() throws ChildProcessTimeoutException, IOException
    {
        super.initializeImpl();
        log("\tchecking if thinpool [%s] exists...", thinPool);
        if (volumeExistsImpl(poolName + "/" + thinPool))
        {
            thinPoolExisted = true;
            log(" yes %n");
        }
        else
        {
            thinPoolExisted = false;
            log(" no %n");
            log("\tcreating thinpool [%s]...", thinPool);
            callChecked("lvcreate", "--size", "100M", "-T", poolName + "/" + thinPool);
            log(" done%n");
        }
    }


    @Override
    protected boolean isVolumeStartStopSupportedImpl()
    {
        return true;
    }

    @Override
    protected boolean isVolumeStartedImpl(String identifier)
    {
        return Files.exists(Paths.get("/","dev", poolName, identifier));
    }

    @Override
    protected void cleanUpImpl() throws ChildProcessTimeoutException, IOException
    {
        inCleanup = true;

        log("cleaning up...%n");
        log("\tchecking test volume(s)... ");
        String[] lvsCommand = new String[]{ "lvs", "-o", "lv_name,lv_path", "--separator", ",", "--noheading" };
        OutputData lvs = callChecked(lvsCommand);
        String[] lines = new String(lvs.stdoutData).split("\n");
        if (testIdentifiers.size() > 0)
        {
            for (String line : lines)
            {
                String[] colums = line.trim().split(",");
                if (testIdentifiers.contains(colums[0]))
                {
                    String identifier = colums[0];
                    log("found volume [%s], trying to remove...", identifier);
                    callChecked("lvremove", "-f", poolName+"/"+identifier);
                    log(" done %n");

                    log("\t\tverifying remove...");
                    lvs = callChecked(lvsCommand);
                    lines = new String(lvs.stdoutData).split("\n");
                    for (String line2 : lines)
                    {
                        if (line2.equals(line))
                        {
                            fail("Failed to remove test volume [%s] in cleanup", identifier);
                        }
                    }
                    testIdentifiers.remove(identifier);
                    log(" done%n");
                    break;
                }
            }
        }
        else
        {
            log("\t\tno test volues used");
        }

        if (!thinPoolExisted)
        {
            log("\tthinpool did not exist before tests - trying to remove...");
            String poolId = poolName + "/" + thinPool;
            callChecked("lvremove", "-f", poolId);
            log(" done %n");
            log("\t\tverifying remove...");
            lvs = callChecked(lvsCommand);
            lines = new String(lvs.stdoutData).split("\n");
            for (String line : lines)
            {
                if (poolId.equals(line.trim()))
                {
                    fail("Failed to remove test thin pool [%s] in cleanup", thinPool);
                }
            }
        }

        if (!poolExisted)
        {
            log("\tpool did not exist before tests - trying to remove...");
            callChecked("vgremove", poolName);
            log(" done %n");

            log("\t\tverifying remove...");
            lvs = callChecked("vgs", "-o", "vg_name", "--noheading");
            lines = new String(lvs.stdoutData).split("\n");
            for (String line : lines)
            {
                if (poolName.equals(line.trim()))
                {
                    fail("Failed to remove test volume group [%s] in cleanup", poolName);
                }
            }
        }
        log("\tall should be cleared now %n%n%n");
    }
}
