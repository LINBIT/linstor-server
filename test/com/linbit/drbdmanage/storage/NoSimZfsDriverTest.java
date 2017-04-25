package com.linbit.drbdmanage.storage;

import java.io.IOException;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd.OutputData;

/**
 * Although this class is in the test folder, this class is
 *
 * NOT A JUNIT TEST.
 *
 * The {@link ZfsDriverTest} is a JUnit test, which simulates every
 * external IO. However, this class will not simulate anything, thus
 * really executing all commands on the underlying system.
 * Because of this, we need to ensure the order of the "tests", which
 * is not possible with JUnit.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class NoSimZfsDriverTest extends NoSimDriverTest
{
    protected String poolName = ZfsDriver.ZFS_POOL_DEFAULT;
    protected boolean poolExisted;


    public NoSimZfsDriverTest() throws IOException, StorageException
    {
        super(new ZfsDriver());
    }

    public NoSimZfsDriverTest(ZfsDriver driver) throws IOException, StorageException
    {
        super(driver);
    }

    public static void main(String[] args) throws IOException, StorageException, ChildProcessTimeoutException
    {
        NoSimZfsDriverTest driverTest = new NoSimZfsDriverTest();
        try
        {
            driverTest.runTest();
        }
        catch (Exception exc)
        {
            System.err.println();
            exc.printStackTrace();
        }
        driverTest.cleanUp();
    }

    @Override
    protected void initialize() throws ChildProcessTimeoutException, IOException
    {
        // ensure that the default pool exists
        log("\tchecking if pool [%s] exists...", poolName);
        OutputData outputData = callChecked("zfs", "list", "-o", "name", "-H");

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
            String blockDevice = "/dev/loop0";
            log("\tcreating pool [%s] on [%s]...", poolName, blockDevice);
            callChecked("zpool", "create", poolName, blockDevice);
            log(" done %n");
        }
    }

    @Override
    protected String getUnusedIdentifier() throws ChildProcessTimeoutException, IOException
    {
        String identifier = null;
        String baseIdentifier = "identifier";
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
        return identifier;
    }

    @Override
    protected boolean volumeExists(String identifier) throws ChildProcessTimeoutException, IOException
    {
        OutputData zfsList = callChecked("zfs", "list", "-o", "name", "-H");
        String zfsOut = new String(zfsList.stdoutData);
        String[] lines = zfsOut.split("\n");

        boolean exists = false;
        for (String line : lines)
        {
            if (line.equals(poolName+"/"+identifier))
            {
                exists = true;
                break;
            }
        }

        return exists;
    }

    @Override
    protected String getVolumePath(String identifier) throws ChildProcessTimeoutException, IOException
    {
        return "/dev/zvol/"+poolName+"/"+identifier;
    }

    @Override
    protected boolean isVolumeStartStopSupported()
    {
        return false;
    }

    @Override
    protected boolean isVolumeStarted(String identifier)
    {
        return false; // will never get called
    }

    @Override
    protected void cleanUp() throws ChildProcessTimeoutException, IOException
    {
        inCleanup = true;

        log("cleaning up...%n");
        String identifier = testIdentifier;
        String[] lvsCommand = new String[]{ "zfs", "list", "-o", "name", "-H"};
        OutputData lvs = callChecked(lvsCommand);
        String[] lines = new String(lvs.stdoutData).split("\n");
        String matchingLine = null;
        log("\tchecking test volume... ");
        for (String line : lines)
        {
            if (identifier.equals(line))
            {
                log("found volume, trying to remove...");
                matchingLine = line;
                callChecked("zfs", "destroy", poolName+"/"+identifier);
                log(" done %n");

                log("\t\tverifying remove...");
                lvs = callChecked(lvsCommand);
                lines = new String(lvs.stdoutData).split("\n");
                for (String line2 : lines)
                {
                    if (line2.equals(matchingLine))
                    {
                        fail("Failed to remove test volume [%s] in cleanup", identifier);
                    }
                }
                break;
            }
        }
        log(" done%n");

        if (!poolExisted)
        {
            log("\tpool did not exist before tests - trying to remove...");
            callChecked("zpool", "destroy", poolName);
            log(" done %n");

            log("\t\tverifying remove...");
            lvs = callChecked("zpool", "list", "-o", "name", "-H");
            lines = new String(lvs.stdoutData).split("\n");
            for (String line : lines)
            {
                if (poolName.equals(line.trim()))
                {
                    fail("Failed to remove test volume group [%s] in cleanup", identifier);
                }
            }
        }
        log("\tall should be cleared now %n%n%n");
    }
}
