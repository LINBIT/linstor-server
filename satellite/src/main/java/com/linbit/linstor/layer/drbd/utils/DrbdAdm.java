package com.linbit.linstor.layer.drbd.utils;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.Platform;
import com.linbit.extproc.ChildProcessHandler.TimeoutType;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdEventService;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdResource;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdStateTracker;
import com.linbit.linstor.layer.drbd.drbdstate.ResourceObserver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.linstor.storage.utils.Commands.RetryHandler;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

@Singleton
public class DrbdAdm
{
    public static final String DRBDADM_UTIL   = "drbdadm";
    public static final String DRBDMETA_UTIL  = "drbdmeta";
    public static final String DRBDSETUP_UTIL = "drbdsetup";

    public static final int DRBDMETA_NO_VALID_MD_RC = 255;
    public static final int DRBDMETA_STRANGE_BM_OFFSET = 1;

    public static final int WAIT_CONNECT_RES_TIME = 10;
    private static final long DOWN_WAIT_TIMEOUT_SEC = 5;

    private final ExtCmdFactory extCmdFactory;
    private final AccessContext sysCtx;
    private final StltConfigAccessor stltCfgAccessor;
    private final DrbdEventService drbdEventService;

    @Inject
    public DrbdAdm(
        ExtCmdFactory extCmdFactoryRef,
        @SystemContext AccessContext sysCtxRef,
        StltConfigAccessor stltCfgAccessorRef,
        DrbdEventService drbdEventServiceRef
    )
    {
        extCmdFactory = extCmdFactoryRef;
        sysCtx = sysCtxRef;
        stltCfgAccessor = stltCfgAccessorRef;
        drbdEventService = drbdEventServiceRef;
    }

    /**
     * Adjusts a resource
     *
     * @throws AccessDeniedException
     */
    public void adjust(
        DrbdRscData<Resource> drbdRscData,
        boolean skipNet,
        boolean skipDisk,
        boolean discard
    )
        throws ExtCmdFailedException, AccessDeniedException
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
        Resource rsc = drbdRscData.getAbsResource();
        PriorityProps prioProps = new PriorityProps(rsc.getProps(sysCtx));
        for (StorPool storPool : LayerVlmUtils.getStorPools(rsc, sysCtx))
        {
            prioProps.addProps(storPool.getProps(sysCtx));
        }
        prioProps.addProps(
            rsc.getNode().getProps(sysCtx),
            rsc.getResourceDefinition().getProps(sysCtx),
            stltCfgAccessor.getReadonlyProps()
        );

        if (
            skipDisk || ApiConsts.VAL_TRUE
                .equals(prioProps.getProp(ApiConsts.KEY_DRBD_SKIP_DISK, ApiConsts.NAMESPC_DRBD_OPTIONS))
        )
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
        DrbdVlmData<Resource> drbdVlmData,
        boolean assumeClean,
        @Nullable Long shrinkToSize
    )
        throws ExtCmdFailedException
    {
        DrbdRscData<Resource> drbdRscData = drbdVlmData.getRscLayerObject();
        waitConnectResource(drbdRscData, WAIT_CONNECT_RES_TIME);
        List<String> command = new ArrayList<>();
        command.addAll(Arrays.asList(DRBDADM_UTIL, "-vvv"));
        if (assumeClean)
        {
            command.add("--");
            command.add("--assume-clean");
        }
        if (shrinkToSize != null)
        {
            command.add("--size=" + Long.toString(shrinkToSize) + "K");
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
     * @param drbdRscData
     * @throws StorageException
     */
    public void down(DrbdRscData<Resource> drbdRscData) throws ExtCmdFailedException, StorageException
    {
        down(drbdRscData, true);
    }

    /**
     * Shuts down (unconfigures) a DRBD resource. Waits for the "destroy resource" event if
     * <code>waitForDestroyEventRef</code> is true
     * @param drbdRscData
     * @param waitForDestroyEventRef
     * @throws StorageException
     */
    public void down(DrbdRscData<Resource> drbdRscData, boolean waitForDestroyEventRef)
        throws ExtCmdFailedException, StorageException
    {
        ArrayBlockingQueue<Boolean> queue = new ArrayBlockingQueue<>(1);
        final String suffixedResourceName = drbdRscData.getSuffixedResourceName();
        final ResourceObserver destroyObserver = new ResourceObserver()
        {
            @Override
            public void resourceDestroyed(DrbdResource resourceRef)
            {
                ResourceObserver.super.resourceDestroyed(resourceRef);
                if (resourceRef.getResName().toString().equals(suffixedResourceName))
                {
                    queue.add(true);
                }
            }
        };
        if (waitForDestroyEventRef)
        {
            drbdEventService.addObserver(destroyObserver, DrbdStateTracker.OBS_RES_DSTR);
        }
        else
        {
            queue.add(true);
        }
        simpleSetupCommand(drbdRscData, null, "down");
        @Nullable Boolean success = null;
        try
        {
            success = queue.poll(DOWN_WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
        }
        catch (InterruptedException exc)
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            drbdEventService.removeObserver(destroyObserver);
        }

        if (success == null || !success)
        {
            throw new StorageException("DRBD resource was not destroyed after " + DOWN_WAIT_TIMEOUT_SEC + " seconds");
        }
    }

    /**
     * Switches a DRBD resource to primary mode
     *
     * @throws StorageException if primary didn't work
     */
    public void primary(
        DrbdRscData<Resource> drbdRscData,
        boolean force,
        boolean withDrbdSetup
    )
        throws StorageException
    {
        List<String> command = new ArrayList<>();
        if (withDrbdSetup)
        {
            command.add(DRBDSETUP_UTIL);
            if (force)
            {
                command.add("--force");
            }
            command.add("primary");
            command.add(drbdRscData.getSuffixedResourceName());
        }
        else
        {
            command.add(DRBDADM_UTIL);
            command.add("-vvv");
            command.add("primary");
            if (force)
            {
                command.add("--force");
            }
            command.add(drbdRscData.getSuffixedResourceName());
        }


        String[] commandArr = command.toArray(new String[0]);
        Commands.genericExecutor(
            extCmdFactory.create(),
            commandArr,
            "Failed to set resource '" + drbdRscData.getSuffixedResourceName() + "' to primary",
            "Failed to set resource '" + drbdRscData.getSuffixedResourceName() + "' to primary",
            new RetryHandler()
            {
                private int retryCount = 3;

                @Override
                public boolean skip(OutputData outDataRef)
                {
                    return false;
                }

                @Override
                public boolean retry(OutputData outputDataRef)
                {
                    String errStr = new String(outputDataRef.stderrData);
                    boolean retryFlag = errStr.contains("Concurrent state changes detected and aborted") &&
                        retryCount > 0;
                    if (retryFlag)
                    {
                        --retryCount;
                        try
                        {
                            Thread.sleep(500);
                        }
                        catch (InterruptedException ignored)
                        {
                        }
                    }
                    return retryFlag;
                }
            }
        );
    }

    /**
     * Switches a DRBD resource to primary mode with auto close (secondary)
     *
     * @throws StorageException If primary command didn't work
     */
    public DrbdPrimary primaryAutoClose(
        DrbdRscData<Resource> drbdRscData,
        boolean force,
        boolean withDrbdSetup
    )
        throws StorageException
    {
        return new DrbdPrimary(this, drbdRscData, force, withDrbdSetup);
    }

    /**
     * Switches a resource to secondary mode
     */
    public void secondary(DrbdRscData<Resource> drbdRscData) throws ExtCmdFailedException
    {
        simpleAdmCommand(drbdRscData, "secondary");
    }

    /**
     * Connects a resource to its peer resuorces on other hosts
     *
     * @throws AccessDeniedException
     */
    public void connect(DrbdRscData<Resource> drbdRscData, boolean discard) throws ExtCmdFailedException,
        AccessDeniedException
    {
        adjust(drbdRscData, false, true, discard);
    }

    /**
     * Disconnects a resource from its peer resources on other hosts
     */
    public void disconnect(DrbdRscData<Resource> drbdRscData) throws ExtCmdFailedException
    {
        simpleAdmCommand(drbdRscData, "disconnect");
    }

    /**
     * Attaches a volume to its disk
     *
     * @throws ExtCmdFailedException
     * @throws AccessDeniedException
     */
    public void attach(DrbdVlmData<Resource> drbdVlmData) throws ExtCmdFailedException, AccessDeniedException
    {
        adjust(drbdVlmData.getRscLayerObject(), true, false, false);
    }

    /**
     * Detaches a volume from its disk
     *
     * @param diskless If true, convert to a diskless client volume
     */
    public void detach(DrbdVlmData<Resource> drbdVlmData, boolean diskless) throws ExtCmdFailedException
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
    public void createMd(DrbdVlmData<Resource> drbdVlmData, int peers) throws ExtCmdFailedException
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

    public void drbdMetaCheckResize(String blockDevPath, int minorNr) throws ExtCmdFailedException
    {
        String[] checkResizeCmd = new String[] {
            DRBDMETA_UTIL,
            Integer.toString(minorNr),
            "v09",
            blockDevPath,
            "internal",
            "check-resize"
        };
        ExtCmd utilsCmd = extCmdFactory.create();

        try
        {
            OutputData outputData = utilsCmd.exec(checkResizeCmd);
            if (outputData.exitCode != 0)
            {
                throw new ExtCmdFailedException(checkResizeCmd, outputData);
            }
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            throw new ExtCmdFailedException(checkResizeCmd, timeoutExc);
        }
        catch (IOException ioExc)
        {
            throw new ExtCmdFailedException(checkResizeCmd, ioExc);
        }
    }

    /**
     * Calls drbdmeta to determine whether DRBD meta data exists on a volume
     */
    public boolean hasMetaData(String blockDevPath, int minorNr, String mdTypeParam) throws ExtCmdFailedException
    {
        // It is probably safer to fail because missing meta data was not detected than
        // to overwrite existing meta data because it was not detected, therefore default
        // to true, indicating that meta data exists
        boolean mdFlag = true;

        String[] params = getGetGiCommand(minorNr, blockDevPath, mdTypeParam);
        ExtCmd utilsCmd = extCmdFactory.create();
        File nullDevice = new File(Platform.nullDevice());
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

    public String getCurrentGID(String blockDevPath, int minorNr, String mdTypeParam) throws ExtCmdFailedException
    {
        String ret;

        String[] params = getGetGiCommand(minorNr, blockDevPath, mdTypeParam);
        ExtCmd utilsCmd = extCmdFactory.create();
        try
        {
            OutputData outputData = utilsCmd.exec(params);
            ret = new String(outputData.stdoutData);
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            throw new ExtCmdFailedException(params, timeoutExc);
        }
        catch (IOException ioExc)
        {
            throw new ExtCmdFailedException(params, ioExc);
        }
        return ret;
    }

    private static String[] getGetGiCommand(int minorNr, String blockDevPath, String mdTypeParam)
    {
        return new String[] {
            DRBDMETA_UTIL,
            Integer.toString(minorNr),
            "v09",
            blockDevPath,
            mdTypeParam,
            "get-gi",
            "--node-id",
            "0",
            "--force"// force is needed if the drbd-resource is still in Negotiating state or earlier.
            // in that case, drbdmeta asks "Exclusive open failed. Do it anyways?" and expects to type 'yes'.
            // should not break anything
        };
    }

    /**
     * Calls drbdmeta to create the metadata information for a volume
     */
    public void setGi(
        NodeId nodeId,
        MinorNumber minorNr,
        String blockDevPath,
        String currentGi,
        @Nullable String history1Gi,
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
    public void showGi(DrbdVlmData<Resource> vlmDataRef) throws ExtCmdFailedException
    {
        DrbdRscData<Resource> rscData = vlmDataRef.getRscLayerObject();
        execute(
            DRBDSETUP_UTIL,
            "show-gi",
            rscData.getSuffixedResourceName(),
            rscData.getNodeId().value + "",
            vlmDataRef.getVlmNr().value + ""
        );
    }

    public void suspendIo(DrbdRscData<Resource> drbdRscData)
        throws ExtCmdFailedException
    {
        execute(DRBDADM_UTIL, "suspend-io", drbdRscData.getSuffixedResourceName());
    }

    public void resumeIo(DrbdRscData<Resource> drbdRscData)
        throws ExtCmdFailedException
    {
        if (drbdRscData.resFileExists())
        {
            execute(DRBDADM_UTIL, "resume-io", drbdRscData.getSuffixedResourceName());
        }
        else
        {
            for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
            {
                execute(
                    DRBDSETUP_UTIL,
                    "resume-io",
                    Integer.toString(drbdVlmData.getVlmDfnLayerObject().getMinorNr().value)
                );
            }
        }
    }

    public void waitConnectResource(DrbdRscData<Resource> drbdRscData, int timeout) throws ExtCmdFailedException
    {
        waitForFamily(drbdRscData, timeout, "wait-connect-resource");
    }

    public void waitSyncResource(DrbdRscData<Resource> drbdRscData, int timeout) throws ExtCmdFailedException
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
        String resPathStr = resPath.toString();

        /* On Windows this is a cygwin path which requires forward slashes. */
        if (Platform.isWindows())
        {
            resPathStr = resPathStr.replace('\\', '/');
        }
        execute(
            DRBDADM_UTIL, "--config-to-test", tmpResPathStr,
            "--config-to-exclude", resPathStr,
            "sh-nop"
        );

        // Using -c disables /etc/drbd.d/global_common.conf
        // execute(DRBDADM_UTIL, "-c", tmpResPathStr, "-d", "up", resourceName.value);
        // execute(DRBDADM_UTIL, "-d", "up", resourceName.value);
    }


    public void deletePeer(DrbdRscData<Resource> peerRscData) throws ExtCmdFailedException
    {
        deletePeer(peerRscData.getSuffixedResourceName(), peerRscData.getNodeId().value);
    }

    public void deletePeer(String suffixedRscName, int nodeId) throws ExtCmdFailedException
    {
        execute(
            DRBDSETUP_UTIL,
            "del-peer",
            suffixedRscName,
            Integer.toString(nodeId)
        );
    }

    public void forgetPeer(DrbdRscData<Resource> rscData) throws ExtCmdFailedException
    {
        forgetPeer(rscData.getSuffixedResourceName(), rscData.getNodeId().value);
    }

    public void forgetPeer(String suffixedRscName, int nodeId) throws ExtCmdFailedException
    {
        execute(
            DRBDSETUP_UTIL,
            "forget-peer",
            suffixedRscName,
            Integer.toString(nodeId)
        );
    }

    public String drbdSetupStatus() throws StorageException
    {
        try
        {
            OutputData drbdStatusCmd = extCmdFactory.create()
                .exec("drbdsetup", "status");
            if (drbdStatusCmd.exitCode != 0)
            {
                throw new StorageException(
                    "Checking drbd state failed: " + new String(drbdStatusCmd.stderrData));
            }
            return new String(drbdStatusCmd.stdoutData);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException("Checking drbd state failed", exc);
        }
    }

    public boolean drbdSetupStatusRscIsUp(String rscName) throws StorageException
    {
        try
        {
            OutputData output = extCmdFactory.create()
                .exec("drbdsetup", "status", rscName);
            return output.exitCode == 0;
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException("Checking drbd state failed", exc);
        }
    }

    private void simpleSetupCommand(
        DrbdRscData<Resource> drbdRscData,
        @Nullable VolumeNumber vlmNr,
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

    private void simpleAdmCommand(DrbdRscData<Resource> drbdRscData, String subcommand) throws ExtCmdFailedException
    {
        simpleAdmCommand(drbdRscData, null, subcommand);
    }

    private void simpleAdmCommand(
        DrbdRscData<Resource> drbdRscData,
        @Nullable VolumeNumber volNum,
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
        DrbdRscData<Resource> drbdRscData,
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

    private void execute(String... command) throws ExtCmdFailedException
    {
        execute(Arrays.asList(command));
    }

    private void execute(List<String> commandList) throws ExtCmdFailedException
    {
        String[] command = commandList.toArray(new String[commandList.size()]);
        try
        {
            File nullDevice = new File(Platform.nullDevice());
            ExtCmd extCmd = extCmdFactory.create();
            if (Platform.isWindows())
            {
                extCmd.setTimeout(TimeoutType.WAIT, 5*60*1000);
            }
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

    public static class DrbdPrimary implements AutoCloseable
    {
        private final DrbdAdm drbdAdm;
        private final DrbdRscData<Resource> drbdRscData;

        public DrbdPrimary(
            DrbdAdm drbdAdmRef, DrbdRscData<Resource> drbdRscDataRef, boolean primary, boolean withDrbdSetup)
            throws StorageException
        {
            this.drbdAdm = drbdAdmRef;
            drbdRscData = drbdRscDataRef;

            drbdAdm.primary(drbdRscData, primary, withDrbdSetup);
        }

        @Override
        public void close() throws ExtCmdFailedException
        {
            drbdAdm.secondary(drbdRscData);
        }
    }
}
