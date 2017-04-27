package com.linbit.drbdmanage.storage;

import java.io.IOException;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd.OutputData;

/**
 * Although this class is in the test folder, this class is
 *
 * NOT A JUNIT TEST.
 *
 * The {@link LvmDriverTest} is a JUnit test, which simulates every
 * external IO. However, this class will not simulate anything, thus
 * really executing all commands on the underlying system.
 * Because of this, we need to ensure the order of the "tests", which
 * is not possible with JUnit.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class NoSimLvmDriverTest extends NoSimDriverTest
{
    protected boolean poolExisted = false;
    protected String poolName = LvmDriver.LVM_VOLUME_GROUP_DEFAULT;

    public NoSimLvmDriverTest() throws IOException, StorageException
    {
        super(new LvmDriver());
    }

    public NoSimLvmDriverTest(StorageDriver driver) throws IOException, StorageException
    {
        super(driver);
    }

    public static void main(String[] args) throws IOException, StorageException, ChildProcessTimeoutException
    {
        NoSimLvmDriverTest driverTest = new NoSimLvmDriverTest();
        try
        {
            driverTest.runTest();
        }
        catch (Exception exc)
        {
            exc.printStackTrace();
        }
        driverTest.cleanUpImpl();
    }

    @Override
    protected void initializeImpl() throws ChildProcessTimeoutException, IOException
    {
        // ensure that the default pool exists
        log("\tchecking if pool [%s] exists...", poolName);
        OutputData outputData = callChecked("vgs", "-o", "vg_name", "--noheading");

        String[] lines = new String(outputData.stdoutData).split("\n");
        for (String line : lines)
        {
            if(line.trim().equals(poolName))
            {
                log(" yes %n");
                poolExisted = true;
                break;
            }
        }
        if (!poolExisted)
        {
            log(" no %n");
            String blockDevice = "/dev/loop1";
            log("\tcreating pool [%s] on [%s]...", poolName, blockDevice);
            callChecked("vgcreate", poolName, blockDevice);
            log(" done %n");
        }
    }

    @Override
    protected String getUnusedIdentifierImpl() throws ChildProcessTimeoutException, IOException
    {
        String identifier = null;
        String baseIdentifier = "identifier";
        int i = 0;
        while (identifier == null)
        {
            identifier = baseIdentifier + i;
            i++;
            if (volumeExistsImpl(identifier))
            {
                identifier = null;
            }
        }
        return identifier;
    }

    @Override
    protected boolean volumeExistsImpl(String identifier) throws ChildProcessTimeoutException, IOException
    {
        boolean exists = false;
        OutputData lvs = callChecked("lvs", "-o", "lv_name", "--noheading");
        String[] lines = new String(lvs.stdoutData).split("\n");
        for (String line : lines)
        {
            if (identifier.equals(line.trim()))
            {
                exists = true;
                break;
            }
        }
        return exists;
    }

    @Override
    protected String getVolumePathImpl(String identifier) throws ChildProcessTimeoutException, IOException
    {
        OutputData lvs = callChecked("lvs", "-o", "lv_name,lv_path", "--separator", ",", "--noheading");
        String[] lines = new String(lvs.stdoutData).split("\n");
        String path = null;
        for (String line : lines)
        {
            String[] colums = line.trim().split(",");
            if (identifier.equals(colums[0]))
            {
                path = colums[1];
                break;
            }
        }
        return path;
    }

    @Override
    protected boolean isVolumeStartStopSupportedImpl()
    {
        return false;
    }

    @Override
    protected boolean isVolumeStartedImpl(String identifier)
    {
        return true; // LvmDriver cannot stop or start
    }

    @Override
    protected void cleanUpImpl() throws ChildProcessTimeoutException, IOException
    {
        inCleanup = true;

        log("cleaning up...%n");
        log("\tchecking test volume(s)... %n");
        String[] lvsCommand = new String[]{ "lvs", "-o", "lv_name,lv_path", "--separator", ",", "--noheading" };
        OutputData lvs = callChecked(lvsCommand);
        String[] lines = new String(lvs.stdoutData).split("\n");
        if (testIdentifiers.size() > 0)
        {
            for (String line : lines)
            {
                String[] colums = line.trim().split(",");
                String identifier = colums[0];
                if (testIdentifiers.contains(identifier))
                {
                    log("\tfound volume [%s], trying to remove...", identifier);
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
                    fail("Failed to remove test volume group(s) [%s] in cleanup", poolName);
                }
            }
        }
        log("\tall should be cleared now %n%n%n");
    }
}
