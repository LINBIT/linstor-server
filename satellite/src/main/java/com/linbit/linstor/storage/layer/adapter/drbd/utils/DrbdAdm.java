package com.linbit.linstor.storage.layer.adapter.drbd.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.extproc.ExtCmdFailedException;

import javax.inject.Inject;
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
    public static final int DRBDMETA_STRANGE_BM_OFFSET = 1;

    public static final int WAIT_CONNECT_RES_TIME = 10;

    private final ExtCmdFactory extCmdFactory;

    @Inject
    public DrbdAdm(ExtCmdFactory extCmdFactoryRef)
    {
        extCmdFactory = extCmdFactoryRef;
    }

    /**
     * Adjusts a resource
     */
    public void adjust(
        DrbdRscData drbdRscData,
        boolean skipNet,
        boolean skipDisk,
        boolean discard
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
        command.add(drbdRscData.getSuffixedResourceName());
        // execute(Arrays.asList("drbdsetup", "show", drbdRscData.getSuffixedResourceName()));
        execute(command);
        // execute(Arrays.asList("drbdsetup", "show", drbdRscData.getSuffixedResourceName()));
    }

    /**
     * Resizes a resource
     */
    public void resize(
        DrbdVlmData drbdVlmData,
        boolean assumeClean
    )
        throws ExtCmdFailedException
    {
        DrbdRscData drbdRscData = drbdVlmData.getRscLayerObject();
        waitConnectResource(drbdRscData, WAIT_CONNECT_RES_TIME);
        List<String> command = new ArrayList<>();
        command.addAll(Arrays.asList(DRBDADM_UTIL, "-vvv"));
        if (assumeClean)
        {
            command.add("--");
            command.add("--assume-clean");
        }
        String resName = drbdRscData.getSuffixedResourceName();
        // Using -c disables /etc/drbd.d/global_common.conf
        // command.addAll(asConfigParameter(resName)); // basically just adds the -c <rscName.conf> as parameter
        command.add("resize");
        command.add(resName + "/" + drbdVlmData.getVlmNr().value);
        execute(command);
    }

    /**
     * Shuts down (unconfigures) a DRBD resource
     * @param rscNameSuffix
     */
    public void down(DrbdRscData drbdRscData) throws ExtCmdFailedException
    {
        simpleSetupCommand(drbdRscData, (VolumeNumber) null, "down");
    }

    /**
     * Switches a DRBD resource to primary mode
     */
    public void primary(
        DrbdRscData drbdRscData,
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
            command.add(drbdRscData.getSuffixedResourceName());

            execute(command);
        }
        else
        {
            if (force)
            {
                simpleAdmCommand(drbdRscData, null, "primary", "--force");
            }
            else
            {
                simpleAdmCommand(drbdRscData, null, "primary");
            }
        }
    }

    /**
     * Switches a resource to secondary mode
     */
    public void secondary(DrbdRscData drbdRscData) throws ExtCmdFailedException
    {
        simpleAdmCommand(drbdRscData, "secondary");
    }

    /**
     * Connects a resource to its peer resuorces on other hosts
     */
    public void connect(DrbdRscData drbdRscData, boolean discard) throws ExtCmdFailedException
    {
        adjust(drbdRscData, false, true, discard);
    }

    /**
     * Disconnects a resource from its peer resources on other hosts
     */
    public void disconnect(DrbdRscData drbdRscData) throws ExtCmdFailedException
    {
        simpleAdmCommand(drbdRscData, "disconnect");
    }

    /**
     * Attaches a volume to its disk
     *
     * @param resourceName
     * @param volNum
     * @throws ExtCmdFailedException
     */
    public void attach(DrbdVlmData drbdVlmData) throws ExtCmdFailedException
    {
        adjust(drbdVlmData.getRscLayerObject(), true, false, false);
    }

    /**
     * Detaches a volume from its disk
     *
     * @param diskless If true, convert to a diskless client volume
     */
    public void detach(DrbdVlmData drbdVlmData, boolean diskless) throws ExtCmdFailedException
    {
        List<String> commands = new ArrayList<>();
        commands.add(DRBDSETUP_UTIL);
        commands.add("detach");
        commands.add(Integer.toString(drbdVlmData.getVlmDfnLayerObject().getMinorNr().value));

        if (diskless)
        {
            commands.add("--diskless");
        }
        execute(commands);
    }

    /**
     * Calls drbdadm to create the metadata information for a volume
     */
    public void createMd(DrbdVlmData drbdVlmData, int peers) throws ExtCmdFailedException
    {
        simpleAdmCommand(
            drbdVlmData.getRscLayerObject(),
            drbdVlmData.getVlmNr(),
            "--max-peers", Integer.toString(peers),
            "--",
            "--force",
            "create-md"
        );
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
        command.add("--force"); // force is needed if the drbd-resource is still in Negotiating state or earlier.
        // in that case, drbdmeta asks "Exclusive open failed. Do it anyways?" and expects to type 'yes'.
        // should not break anything

        boolean mdFlag = true;
        String[] params = command.toArray(new String[command.size()]);
        ExtCmd utilsCmd = extCmdFactory.create();
        File nullDevice = new File("/dev/null");
        try
        {
            OutputData outputData = utilsCmd.pipeExec(ProcessBuilder.Redirect.from(nullDevice), params);
            if (outputData.exitCode == DRBDMETA_NO_VALID_MD_RC || outputData.exitCode == DRBDMETA_STRANGE_BM_OFFSET)
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
        boolean upToDateData,
        boolean internal
    )
        throws ExtCmdFailedException
    {
        String giData = currentGi + ":";
        if (upToDateData || history1Gi != null)
        {
            String checkedHistory1Gi = history1Gi == null ? "0" : history1Gi;
            giData += "0:" + checkedHistory1Gi + ":0:";
            if (upToDateData)
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
            internal ? "internal" : "flex-external",
            "set-gi", giData
        );
    }

    /**
     * For debug purposes
     *
     * @param vlmDataRef
     * @throws ExtCmdFailedException
     */
    public void showGi(DrbdVlmData vlmDataRef) throws ExtCmdFailedException
    {
        DrbdRscData rscData = vlmDataRef.getRscLayerObject();
        execute(
            DRBDSETUP_UTIL,
            "show-gi",
            rscData.getSuffixedResourceName(),
            rscData.getNodeId().value + "",
            vlmDataRef.getVlmNr().value + ""
        );
    }

    public void suspendIo(DrbdRscData drbdRscData)
        throws ExtCmdFailedException
    {
        execute(DRBDADM_UTIL, "suspend-io", drbdRscData.getSuffixedResourceName());
    }

    public void resumeIo(DrbdRscData drbdRscData)
        throws ExtCmdFailedException
    {
        execute(DRBDADM_UTIL, "resume-io", drbdRscData.getSuffixedResourceName());
    }

    public void waitConnectResource(DrbdRscData drbdRscData, int timeout) throws ExtCmdFailedException
    {
        waitForFamily(drbdRscData, timeout, "wait-connect-resource");
    }

    public void waitSyncResource(DrbdRscData drbdRscData, int timeout) throws ExtCmdFailedException
    {
        waitForFamily(drbdRscData, timeout, "wait-sync-resource");
    }

    public void checkResFile(
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
        // execute(DRBDADM_UTIL, "-d", "up", resourceName.value);
    }


    public void deletePeer(DrbdRscData peerRscData) throws ExtCmdFailedException
    {
        execute(
            DRBDSETUP_UTIL,
            "del-peer",
            peerRscData.getSuffixedResourceName(),
            Integer.toString(peerRscData.getNodeId().value)
        );
    }

    public void forgetPeer(DrbdRscData rscData) throws ExtCmdFailedException
    {
        execute(
            DRBDSETUP_UTIL,
            "forget-peer",
            rscData.getSuffixedResourceName(),
            Integer.toString(rscData.getNodeId().value)
        );
    }

    private void simpleSetupCommand(
        DrbdRscData drbdRscData,
        VolumeNumber vlmNr,
        String... subCommands
    )
        throws ExtCmdFailedException
    {
        List<String> command = new ArrayList<>();
        command.add(DRBDSETUP_UTIL);
        command.addAll(Arrays.asList(subCommands));

        String drbdObj = drbdRscData.getSuffixedResourceName();
        if (vlmNr != null)
        {
            drbdObj += "/" + vlmNr.value;
        }
        command.add(drbdObj);

        execute(command);
    }

    private void simpleAdmCommand(DrbdRscData drbdRscData, String subcommand) throws ExtCmdFailedException
    {
        simpleAdmCommand(drbdRscData, null, subcommand);
    }

    private void simpleAdmCommand(
        DrbdRscData drbdRscData,
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
        String resName = drbdRscData.getSuffixedResourceName();
        if (volNum != null)
        {
            resName += "/" + volNum.value;
        }
        command.add(resName);

        execute(command);
    }

    private void waitForFamily(
        DrbdRscData drbdRscData,
        int timeout,
        String commandRef
    )
        throws ExtCmdFailedException
    {
        execute(
            DRBDSETUP_UTIL,
            commandRef,
            "--wait-after-sb=yes",
            "--wfc-timeout=" + timeout,
            drbdRscData.getSuffixedResourceName()
        );
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
            ExtCmd extCmd = extCmdFactory.create();
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
