package com.linbit.drbd;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.core.SatelliteCoreModule;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.timer.CoreTimer;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;

@Singleton
public class DrbdAdm
{
    public static final String DRBDADM_UTIL   = "drbdadm";
    public static final String DRBDMETA_UTIL  = "drbdmeta";
    public static final String DRBDSETUP_UTIL = "drbdsetup";

    public static final String ALL_KEYWORD = "all";
    public static final String DRBDCTRL_RES_NAME = ".drbdctrl";

    public static final int DRBDMETA_NO_VALID_MD_RC = 255;

    public static final int WAIT_CONNECT_RES_TIME = 10;

    private final ErrorReporter errorReporter;
    private final Path configPath;
    private final CoreTimer timer;

    @Inject
    public DrbdAdm(
        ErrorReporter errorReporterRef,
        @Named(SatelliteCoreModule.DRBD_CONFIG_PATH) Path configPathRef,
        CoreTimer timerRef
    )
    {
        errorReporter = errorReporterRef;
        configPath = configPathRef;
        timer = timerRef;
    }

    /**
     * Adjusts a resource
     */
    public void adjust(
        ResourceName resourceName,
        boolean skipNet,
        boolean skipDisk,
        boolean discard,
        VolumeNumber volNum
    )
        throws ExtCmdFailedException
    {
        List<String> command = new ArrayList<>();
        command.addAll(Arrays.asList(DRBDADM_UTIL, "-vvv", "adjust"));

        // Using -c disables /etc/drbd.d/global_common.conf
        // command.add("-c");
        // command.add("/etc/drbd.d/" + resourceName.displayValue + ".res");

        if (discard)
        {
            command.add("--discard-my-data");
        }
        if (skipNet)
        {
            command.add("--skip-net");
        }
        if (skipDisk)
        {
            command.add("--skip-disk");
        }
        // String resName = resourceName.value;
        // command.addAll(asConfigParameter(resName));
        // if (volNum != null)
        // {
        //     resName += "/" + volNum.value;
        // }
        // command.add(resName);
        command.add(resourceName.displayValue);
        execute(command);
    }

    /**
     * Resizes a resource
     */
    public void resize(
        ResourceName resourceName,
        VolumeNumber volNum,
        boolean assumeClean
    )
        throws ExtCmdFailedException
    {
        waitConnectResource(resourceName, WAIT_CONNECT_RES_TIME);
        List<String> command = new ArrayList<>();
        command.addAll(Arrays.asList(DRBDADM_UTIL, "-vvv"));
        if (assumeClean)
        {
            command.add("--");
            command.add("--assume-clean");
        }
        String resName = resourceName.value;
        // Using -c disables /etc/drbd.d/global_common.conf
        // command.addAll(asConfigParameter(resName)); // basically just adds the -c <rscName.conf> as parameter
        command.add("resize");
        command.add(resName + "/" + volNum.value);
        execute(command);
    }

    /**
     * Shuts down (unconfigures) a DRBD resource
     */
    public void down(ResourceName resourceName) throws ExtCmdFailedException
    {
        simpleSetupCommand(resourceName, "down");
    }

    /**
     * Switches a DRBD resource to primary mode
     */
    public void primary(
        ResourceName resourceName,
        boolean force,
        boolean withDrbdSetup
    )
        throws ExtCmdFailedException
    {
        if (withDrbdSetup)
        {
            List<String> command = new ArrayList<>();
            command.add(DRBDSETUP_UTIL);
            if (force)
            {
                command.add("--force");
            }
            command.add("primary");
            command.add(resourceName.value);

            execute(command);
        }
        else
        {
            if (force)
            {
                simpleAdmCommand(resourceName, null, "primary", "--force");
            }
            else
            {
                simpleAdmCommand(resourceName, null, "primary");
            }
        }
    }

    /**
     * Switches a resource to secondary mode
     */
    public void secondary(ResourceName resourceName) throws ExtCmdFailedException
    {
        simpleAdmCommand(resourceName, "secondary");
    }

    /**
     * Connects a resource to its peer resuorces on other hosts
     */
    public void connect(ResourceName resourceName, boolean discard) throws ExtCmdFailedException
    {
        adjust(resourceName, false, true, discard, null);
    }

    /**
     * Disconnects a resource from its peer resources on other hosts
     */
    public void disconnect(ResourceName resourceName) throws ExtCmdFailedException
    {
        simpleAdmCommand(resourceName, "disconnect");
    }

    /**
     * Attaches a volume to its disk
     *
     * @param resourceName
     * @param volNum
     * @throws ExtCmdFailedException
     */
    public void attach(ResourceName resourceName, VolumeNumber volNum) throws ExtCmdFailedException
    {
        adjust(resourceName, true, false, false, volNum);
    }

    /**
     * Detaches a volume from its disk
     */
    public void detach(ResourceName resourceName, VolumeNumber volNum) throws ExtCmdFailedException
    {
        simpleAdmCommand(resourceName, volNum, "detach");
    }

    /**
     * Calls drbdadm to create the metadata information for a volume
     */
    public void createMd(ResourceName resourceName, VolumeNumber volNum, int peers) throws ExtCmdFailedException
    {
        simpleAdmCommand(resourceName, volNum, "--max-peers", Integer.toString(peers), "--", "--force", "create-md");
    }

    /**
     * Calls drbdmeta to determine whether DRBD meta data exists on a volume
     */
    public boolean hasMetaData(String blockDevPath, int minorNr, String mdTypeParam) throws ExtCmdFailedException
    {
        // It is probably safer to fail because missing meta data was not detected than
        // to overwrite existing meta data because it was not detected, therefore default
        // to true, indicating that meta data exists
        List<String> command = new ArrayList<>();
        command.add(DRBDMETA_UTIL);
        command.add(Integer.toString(minorNr));
        command.add("v09");
        command.add(blockDevPath);
        command.add(mdTypeParam);
        command.add("get-gi");
        command.add("--node-id");
        command.add("0");

        boolean mdFlag = true;
        String[] params = command.toArray(new String[command.size()]);
        ExtCmd utilsCmd = new ExtCmd(timer, errorReporter);
        File nullDevice = new File("/dev/null");
        try
        {
            OutputData outputData = utilsCmd.pipeExec(ProcessBuilder.Redirect.from(nullDevice), params);
            if (outputData.exitCode == DRBDMETA_NO_VALID_MD_RC)
            {
                mdFlag = false;
            }
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            throw new ExtCmdFailedException(params, timeoutExc);
        }
        catch (IOException ioExc)
        {
            throw new ExtCmdFailedException(params, ioExc);
        }
        return mdFlag;
    }

    /**
     * Calls drbdmeta to create the metadata information for a volume
     */
    public void setGi(
        NodeId nodeId,
        MinorNumber minorNr,
        String blockDevPath,
        String currentGi,
        String history1Gi,
        boolean setFlags
    )
        throws ExtCmdFailedException
    {
        String giData = currentGi + ":";
        if (setFlags || history1Gi != null)
        {
            String checkedHistory1Gi = history1Gi == null ? "0" : history1Gi;
            giData += "0:" + checkedHistory1Gi + ":0:";
            if (setFlags)
            {
                giData += "1:1:";
            }
        }
        execute(
            DRBDMETA_UTIL,
            "--force",
            "--node-id", Integer.toString(nodeId.value),
            String.valueOf(minorNr.value),
            "v09",
            blockDevPath,
            "internal",
            "set-gi", giData
        );
    }

    /**
     * Calls drbdadm to set a new current GI
     */
    public void newCurrentUuid(ResourceName resourceName, VolumeNumber volNum) throws ExtCmdFailedException
    {
        simpleAdmCommand(resourceName, volNum, "--clear-bitmap", "new-current-uuid");
    }

    public void waitConnectResource(ResourceName resourceName, int timeout) throws ExtCmdFailedException
    {
        waitForFamily(resourceName, timeout, "wait-connect-resource");
    }

    public void waitSyncResource(ResourceName resourceName, int timeout) throws ExtCmdFailedException
    {
        waitForFamily(resourceName, timeout, "wait-sync-resource");
    }

    public void checkResFile(
        ResourceName resourceName,
        Path tmpResPath,
        Path resPath
    )
        throws ExtCmdFailedException
    {
        String tmpResPathStr = tmpResPath.toString();
        execute(
            DRBDADM_UTIL, "--config-to-test", tmpResPathStr,
            "--config-to-exclude", resPath.toString(),
            "sh-nop"
        );

        // Using -c disables /etc/drbd.d/global_common.conf
        // execute(DRBDADM_UTIL, "-c", tmpResPathStr, "-d", "up", resourceName.value);
        execute(DRBDADM_UTIL, "-d", "up", resourceName.value);
    }

    private void simpleSetupCommand(ResourceName rscName, String subcommand) throws ExtCmdFailedException
    {
        simpleSetupCommand(rscName, null, subcommand);
    }

    private void simpleSetupCommand(ResourceName rscName, VolumeNumber vlmNr, String... subCommands)
        throws ExtCmdFailedException
    {
        List<String> command = new ArrayList<>();
        command.add(DRBDSETUP_UTIL);
        command.addAll(Arrays.asList(subCommands));

        String drbdObj = rscName.displayValue;
        if (vlmNr != null)
        {
            drbdObj += "/" + vlmNr.value;
        }
        command.add(drbdObj);

        execute(command);
    }

    private void simpleAdmCommand(ResourceName resourceName, String subcommand) throws ExtCmdFailedException
    {
        simpleAdmCommand(resourceName, null, subcommand);
    }

    private void simpleAdmCommand(
        ResourceName resourceName,
        VolumeNumber volNum,
        String... subCommands
    )
        throws ExtCmdFailedException
    {
        List<String> command = new ArrayList<>();
        command.add(DRBDADM_UTIL);
        command.add("-vvv");

        // Using -c disables /etc/drbd.d/global_common.conf
        // command.add("-c");
        // command.add("/etc/drbd.d/" + resourceName.displayValue + ".res");

        // command.addAll(asConfigParameter(resourceName.value));
        command.addAll(Arrays.asList(subCommands));
        String resName = resourceName.displayValue;
        if (volNum != null)
        {
            resName += "/" + volNum.value;
        }
        command.add(resName);

        execute(command);
    }

    private void waitForFamily(
        ResourceName resourceName,
        int timeout,
        String commandRef
    )
        throws ExtCmdFailedException
    {
        execute(DRBDSETUP_UTIL, commandRef, "--wait-after-sb=yes", "--wfc-timeout=" + timeout, resourceName.value);
    }

    // Using -c disables /etc/drbd.d/global_common.conf
    // private List<String> asConfigParameter(String resourceName)
    // {
    //    String[] ret;
    //    if (!resourceName.toLowerCase().equals(ALL_KEYWORD) &&
    //        !resourceName.equals(DRBDCTRL_RES_NAME))
    //    {
    //       String resourcePath = configPath.resolve("linstor_" + resourceName + ".res").toString();
    //       ret = new String[]
    //       {
    //           "-c", resourcePath
    //       };
    //    }
    //    else
    //    {
    //       ret = new String[0];
    //    }
    //    return Arrays.asList(ret);
    // }

    private void execute(String... command) throws ExtCmdFailedException
    {
        execute(Arrays.asList(command));
    }

    private void execute(List<String> commandList) throws ExtCmdFailedException
    {
        String[] command = commandList.toArray(new String[commandList.size()]);
        try
        {
            // FIXME: Works only on Unix
            File nullDevice = new File("/dev/null");
            ExtCmd extCmd = new ExtCmd(timer, errorReporter);
            OutputData outputData = extCmd.pipeExec(ProcessBuilder.Redirect.from(nullDevice), command);
            if (outputData.exitCode != 0)
            {
                throw new ExtCmdFailedException(command, outputData);
            }
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            throw new ExtCmdFailedException(command, timeoutExc);
        }
        catch (IOException ioExc)
        {
            throw new ExtCmdFailedException(command, ioExc);
        }
    }
}
