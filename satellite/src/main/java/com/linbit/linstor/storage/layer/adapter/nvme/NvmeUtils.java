package com.linbit.linstor.storage.layer.adapter.nvme;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;

import static com.linbit.linstor.api.ApiConsts.KEY_PREF_NIC;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Class for processing NvmeRscData
 *
 * @author Rainer Laschober
 * @since v0.9.6
 */
@Singleton
public class NvmeUtils
{
    public static final String NVME_SUBSYSTEM_PREFIX = "LS-NVMe_";
    private static final String NVMET_PATH = "/sys/kernel/config/nvmet/";
    public static final String NVME_SUBSYSTEMS_PATH = NVMET_PATH + "subsystems/";
    private static final String NVME_PORTS_PATH = NVMET_PATH + "ports/";
    private static final String NVME_FABRICS_PATH = "/sys/devices/virtual/nvme-fabrics/ctl/nvme";

    private static final int IANA_DEFAULT_PORT = 4420;
    private static final int NVME_IDX_MAX_DIGITS = 5;
    private static final int NVME_GREP_SLEEP_MAX_WAIT_TIME = 50;
    private static final long NVME_GREP_SLEEP_INCREMENT = 200L;

    private final ExtCmdFactory extCmdFactory;
    private final Props stltProps;
    private final ErrorReporter errorReporter;

    @Inject
    public NvmeUtils(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltPropsRef
    )
    {
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        stltProps = stltPropsRef;
    }


    /* compute methods */

    /**
     * Creates the necessary directories and files for the NVMe subsystem and namespaces
     * depending on the {@link NvmeRscData} and {@link NvmeVlmData}.
     *
     * @param nvmeRscData   NvmeRscData object containing all needed information for this method
     * @param accCtx        AccessContext needed to access properties and the IP address
     */
    public void createTargetRsc(NvmeRscData nvmeRscData, AccessContext accCtx)
        throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();
        final String subsystemName = NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName();
        final String subsystemDirectory = NVME_SUBSYSTEMS_PATH + subsystemName;
        final Path subsystemPath = Paths.get(subsystemDirectory);

        try
        {
            errorReporter.logDebug(
                "NVMe: creating subsystem on target: " + NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
            );

            final PriorityProps nvmePrioProps = new PriorityProps(
                nvmeRscData.getResource().getDefinition().getProps(accCtx),
                stltProps
            );

            // create nvmet-rdma subsystem
            Files.createDirectories(subsystemPath);

            // allow any host to be connected
            Files.write(subsystemPath.resolve("attr_allow_any_host"), "1".getBytes());

            for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
            {
                final Path namespacePath = Paths.get(
                    subsystemDirectory + "/namespaces/" + (nvmeVlmData.getVlmNr().getValue() + 1)
                );

                // create namespace, set path to nvme device and enable namespace
                createNamespace(namespacePath, nvmeVlmData.getBackingDevice().getBytes());
            }

            // get port directory or create it if the first subsystem is being added
            LsIpAddress ipAddr = getIpAddr(nvmeRscData.getResource(), accCtx);
            String portIdx = getPortIdx(ipAddr, extCmd);

            if (portIdx == null)
            {
                errorReporter.logDebug("NVMe: creating new ports directory on target");

                // get existing port directories and compute next available index
                OutputData output = extCmd.exec("/bin/bash", "-c", "ls -m --color=never " + NVME_PORTS_PATH);
                ExtCmdUtils.checkExitCode(
                    output,
                    StorageException::new,
                    "Failed to list files!"
                );
                String outputStr = new String(output.stdoutData);
                if (outputStr.trim().isEmpty())
                {
                    portIdx = "1";
                }
                else
                {
                    String[] portDirs = outputStr.split(", ");
                    portIdx = Integer.toString(
                        Integer.parseInt(portDirs[portDirs.length - 1].trim()) + 1
                    );
                }

                Path portsPath = Paths.get(NVME_PORTS_PATH + portIdx);
                Files.createDirectories(portsPath);
                Files.write(
                    portsPath.resolve("addr_traddr"), ipAddr.getAddress().getBytes()
                    );

                // set the transport type and port
                String transportType = nvmePrioProps.getProp(ApiConsts.KEY_TR_TYPE);
                if (transportType == null)
                {
                    transportType = "rdma";
                }
                Files.write(portsPath.resolve("addr_trtype"), transportType.getBytes());

                String port = nvmePrioProps.getProp(ApiConsts.KEY_PORT);
                if (port == null)
                {
                    port = Integer.toString(IANA_DEFAULT_PORT);
                }
                Files.write(portsPath.resolve("addr_trsvcid"), port.getBytes());

                // set the address family of the port, either IPv4 or IPv6
                Files.write(
                    portsPath.resolve("addr_adrfam"),
                    ipAddr.getAddressType().toString().toLowerCase().getBytes()
                );
            }
            // create soft link
            ExtCmdUtils.checkExitCode(
                extCmd.exec(
                    "ln",
                    "-s",
                    subsystemDirectory,
                    NVME_PORTS_PATH + portIdx + "/subsystems/" + subsystemName
                    ),
                StorageException::new,
                "Failed to create symbolic link!"
            );
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to configure NVMe target!", exc);
        }
        catch (InvalidNameException | AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Reverses the operations executed by createTargetRsc(), thus deleting the data on the NVMe Target
     *
     * @param nvmeRscData   NvmeRscData object containing all needed information for this method
     * @param accCtx        AccessContext needed to access properties and the IP address
     */
    public void deleteTargetRsc(NvmeRscData nvmeRscData, AccessContext accCtx)
        throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();
        final String subsystemName = NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName();
        final String subsystemDirectory = NVME_SUBSYSTEMS_PATH + subsystemName;

        try
        {
            errorReporter.logDebug(
                "NVMe: cleaning up target: " + NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
            );

            // remove soft link
            String portIdx = getPortIdx(getIpAddr(nvmeRscData.getResource(), accCtx), extCmd);
            if (portIdx == null)
            {
                throw new StorageException(
                    "No ports directory for NVMe subsystem \'" + subsystemName + "\' exists!"
                );
            }

            OutputData output = extCmd.exec(
                "rm", NVME_PORTS_PATH + portIdx + "/subsystems/" + subsystemName
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to remove symbolic link!");

            // delete ports directory
            output = extCmd.exec(
                "/bin/bash", "-c", "ls -m --color=never " + NVME_PORTS_PATH + portIdx + "/subsystems"
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to list files!");

            if (new String(output.stdoutData).trim().isEmpty())
            {
                output = extCmd.exec("rmdir", NVME_PORTS_PATH + portIdx);
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete ports directory!");
            }

            for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
            {
                final int namespaceNr = nvmeVlmData.getVlmNr().getValue() + 1;
                final Path namespacePath = Paths.get(
                    subsystemDirectory + "/namespaces/" + namespaceNr
                );

                errorReporter.logDebug("NVMe: deleting namespace: " + namespaceNr);

                // delete namespace
                deleteNamespace(namespacePath, extCmd);
            }

            // delete subsystem directory
            output = extCmd.exec("rmdir", subsystemDirectory);
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete subsystem directory!");
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to delete NVMe target!", exc);
        }
        catch (InvalidNameException | AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Connects the NVMe Initiator to the Target after discovering available subsystems, given the subsystem name
     *
     * @param nvmeRscData   NvmeRscData object containing all needed information for this method
     * @param accCtx        AccessContext needed to access properties and Target resource
     */
    public void connect(NvmeRscData nvmeRscData, AccessContext accCtx)
        throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();
        final String subsystemName = NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName();

        try
        {
            errorReporter.logDebug(
                "NVMe: connecting initiator to: " + NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
            );

            final PriorityProps nvmePrioProps = new PriorityProps(
                nvmeRscData.getResource().getDefinition().getProps(accCtx),
                stltProps
            );

            String port = nvmePrioProps.getProp(ApiConsts.KEY_PORT);
            if (port == null)
            {
                port = Integer.toString(IANA_DEFAULT_PORT);
            }
            String transportType = nvmePrioProps.getProp(ApiConsts.KEY_TR_TYPE);
            if (transportType == null)
            {
                transportType = "rdma";
            }

            String ipAddr = getIpAddr(// TODO: check on controller
                nvmeRscData.getResource().getDefinition().streamResource(accCtx).filter(
                    rsc -> !rsc.equals(nvmeRscData.getResource())
                ).findFirst().orElseThrow(() -> new StorageException("Target resource not found!")),
                accCtx
            ).getAddress();

            List<String> subsystemNames = discover(transportType, ipAddr, port, extCmd);
            if (!subsystemNames.contains(subsystemName))
            {
                throw new StorageException("Failed to discover subsystem name \'" + subsystemName + "\'!");
            }

            OutputData output = extCmd.exec(
                "nvme",
                "connect",
                "-t",
                transportType,
                "-n",
                subsystemName,
                "-a",
                ipAddr,
                "-s",
                port
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to connect to NVMe target!");
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to connect to NVMe target!", exc);
        }
        catch (InvalidNameException | AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Disconnects the NVMe Initiator from the Target, given the subsystem name
     *
     * @param nvmeRscData NvmeRscData object containing all needed information for this method
     */
    public void disconnect(NvmeRscData nvmeRscData) throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();

        try
        {
            errorReporter.logDebug(
                "NVMe: disconnecting initiator from: " + NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
            );

            OutputData output = extCmd.exec(
                "nvme", "disconnect", "-n",
                NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to disconnect from NVMe target!");
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to disconnect from NVMe target!", exc);
        }
    }

    /**
     * Checks whether the specified subsystem directory exists
     *
     * @param nvmeRscData   NvmeRscData object containing all needed information for this method
     * @return              boolean true if the subsystem directory exists and false otherwise
     */
    public boolean isTargetConfigured(NvmeRscData nvmeRscData)
    {
        errorReporter.logDebug(
            "NVMe: checking if subsystem " + NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName() + " exists."
        );

        return Files.exists(Paths.get(NVME_SUBSYSTEMS_PATH).resolve(
            NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName())
        );
    }

    /**
     * Queries NVMe-specific indices for the {@link NvmeRscData} and {@link NvmeVlmData}
     * and stores the result as the device path (for example '/dev/nvme2n1')
     *
     * @param nvmeRscData   NvmeRscData object containing all needed information for this method
     * @param isWaiting     boolean true if the external commands should be waited for in loop
     * @return              boolean true if the data was found and false otherwise
     */
    public boolean setDevicePaths(NvmeRscData nvmeRscData, boolean isWaiting)
        throws StorageException
    {
        boolean success = true;
        final ExtCmd extCmd = extCmdFactory.create();
        final String subsystemName = NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName();

        try
        {
            errorReporter.logDebug(
                "NVMe: trying to set device paths for: " + NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
            );

            OutputData output = executeCmdAfterWaiting(extCmd, isWaiting,
                "/bin/bash", "-c", "grep -H -r " + subsystemName + " " + NVME_FABRICS_PATH + "*/subsysnqn"
            );
            if (output == null)
            {
                success = false;
            }
            else
            {
                final int nvmeRscIdx = Integer.parseInt(
                    (new String(output.stdoutData))
                        .substring(NVME_FABRICS_PATH.length(), NVME_FABRICS_PATH.length() + NVME_IDX_MAX_DIGITS)
                        .split(File.separator)[0]
                );
                final String nvmeFabricsVlmPath = NVME_FABRICS_PATH + nvmeRscIdx + "/nvme" + nvmeRscIdx;

                for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
                {
                    output = executeCmdAfterWaiting(extCmd, isWaiting,
                        "/bin/bash", "-c",
                        " grep -H -r " + (nvmeVlmData.getVlmNr().getValue() + 1) + " " +
                            nvmeFabricsVlmPath + "c*n*/nsid");

                    if (output == null)
                    {
                        success = false;
                    }
                    else
                    {
                        String fabricsPathRsc = nvmeFabricsVlmPath + "c*n";

                        final int nvmeVlmIdx = Integer.parseInt(
                            (new String(output.stdoutData))
                                .substring(fabricsPathRsc.length(), fabricsPathRsc.length() + NVME_IDX_MAX_DIGITS)
                                .split(File.separator)[0]
                        );

                        nvmeVlmData.setDevicePath("/dev/nvme" + nvmeRscIdx + "n" + nvmeVlmIdx);
                    }
                }
            }
        }
        catch (IOException | ChildProcessTimeoutException | InterruptedException exc)
        {
            throw new StorageException("Failed to set NVMe device path!", exc);
        }

        return success;
    }

    /**
     * Creates a new directory for the given file path, sets the appropriate backing device and enables the namespace
     *
     * @param namespacePath Path to the new namespace
     * @param backingDevice byte[] containing the device path
     */
    public void createNamespace(Path namespacePath, byte[] backingDevice)
        throws IOException
    {
        if (!Files.exists(namespacePath))
        {
            errorReporter.logDebug("NVMe: creating namespace: " + namespacePath.getFileName());
            Files.createDirectories(namespacePath);
            Files.write(namespacePath.resolve("device_path"), backingDevice);
            Files.write(namespacePath.resolve("enable"), "1".getBytes());
        }
    }

    /**
     * Disables the namespace at the given file path and deletes the directory
     *
     * @param namespacePath Path to the new namespace
     * @param extCmd        ExtCmd for executing commands
     */
    public void deleteNamespace(Path namespacePath, ExtCmd extCmd)
        throws IOException, ChildProcessTimeoutException, StorageException
    {
        if (Files.exists(namespacePath))
        {
            errorReporter.logDebug("NVMe: deleting namespace: " + namespacePath.getFileName());
            Files.write(namespacePath.resolve("enable"), "0".getBytes());
            OutputData output = extCmd.exec("rmdir", namespacePath.toString());
            ExtCmdUtils.checkExitCode(
                output,
                StorageException::new,
                "Failed to delete namespace directory!"
            );
        }
    }


    /* helper methods */

    /**
     * Executes the given command immediately or waits for its completion, depending on {@param isWaiting}
     *
     * @param extCmd    ExtCmd for executing commands
     * @param isWaiting boolean true if the external commands should be waited for in loop
     * @param command   String... command being executed
     * @return          OutputData of the executed command or null if something went wrong
     */
    private OutputData executeCmdAfterWaiting(ExtCmd extCmd, boolean isWaiting, String... command)
        throws IOException, ChildProcessTimeoutException, InterruptedException
    {
        OutputData output = null;
        int tries;
        boolean extCmdSuccess;
        if (isWaiting)
        {
            tries = 0;
        }
        else
        {
            tries = NVME_GREP_SLEEP_MAX_WAIT_TIME - 1;
        }
        do
        {
            output = extCmd.exec(command);
            extCmdSuccess = output.exitCode == 0;
            if (!extCmdSuccess)
            {
                Thread.sleep(NVME_GREP_SLEEP_INCREMENT);
                tries++;
            }
        }
        while (!extCmdSuccess && tries < NVME_GREP_SLEEP_MAX_WAIT_TIME);

        if (isWaiting && tries >= NVME_GREP_SLEEP_MAX_WAIT_TIME || !extCmdSuccess)
        {
            output = null;
        }
        return output;
    }

    /**
     * Executes the nvme-discover command and reads the names of available subsystems from the output
     *
     * @param transportType String, only RDMA featured at the moment
     * @param ipAddr        String, can be IPv4 or IPv6
     * @param port          String, default: 4420
     * @param extCmd        ExtCmd for executing commands
     * @return              List<String> of discovered subsystem names
     */
    private List<String> discover(String transportType, String ipAddr, String port, ExtCmd extCmd)
        throws StorageException
    {
        List<String> subsystemNames = new ArrayList<>();

        try
        {
            errorReporter.logDebug("NVMe: discovering target subsystems.");

            OutputData output = extCmd.exec(
                "nvme",
                "discover",
                "-t",
                transportType,
                "-a",
                ipAddr,
                "-s",
                port
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to discover NVMe subsystems!");

            for (String outputLine : (new String(output.stdoutData)).split("\n"))
            {
                if (outputLine.contains("subnqn:"))
                {
                    subsystemNames.add(outputLine.substring(outputLine.indexOf(NVME_SUBSYSTEM_PREFIX)));
                }
            }
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to discover NVMe subsystems!", exc);
        }

        return subsystemNames;
    }

    /**
     * Queries the resource's net interface for its IP address.
     * If no preferred net interface is configured, the default net interface is assigned
     *
     * @param rsc       Resource object containing all needed information for this method
     * @param accCtx    AccessContext needed to access properties and the net interface
     * @return          {@link LsIpAddress} of the resource's net interface
     */
    private LsIpAddress getIpAddr(Resource rsc, AccessContext accCtx)
        throws StorageException, InvalidNameException, AccessDeniedException, InvalidKeyException
    {
        PriorityProps prioProps = new PriorityProps();
        Iterator<Volume> iterateVolumes = rsc.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();
            prioProps.addProps(vlm.getStorPool(accCtx).getProps(accCtx));
            prioProps.addProps(vlm.getProps(accCtx));
        }
        prioProps.addProps(rsc.getProps(accCtx));
        prioProps.addProps(rsc.getAssignedNode().getProps(accCtx));
        prioProps.addProps(stltProps);

        String netIfName = prioProps.getProp(KEY_PREF_NIC, ApiConsts.NAMESPC_NVME);

        NetInterface netIf;
        if (netIfName == null)
        {
            Iterator<NetInterface> iterateNetInterfaces = rsc.getAssignedNode().iterateNetInterfaces(accCtx);
            if (iterateNetInterfaces.hasNext())
            {
                netIf = iterateNetInterfaces.next();
            }
            else
            {
                throw new StorageException("No network interface defined for node " + rsc.getAssignedNode().getName());
            }
        }
        else
        {
            errorReporter.logDebug("NVMe: querying net interface " + netIfName + "for IP address.");

            Node rscNode = rsc.getAssignedNode();
            netIf = rscNode.getNetInterface(accCtx, new NetInterfaceName(netIfName));
            if (netIf == null)
            {
                throw new StorageException(
                    "The preferred network interface '" + netIfName + "' of node '" +
                        rscNode.getName() + "' does not exist!"
                ); // TODO: call checkPrefNic() when rsc.getAssignedNode().getNetInterface(accCtx, new NetInterfaceName(netIfName)) is called
            }
        }

        return netIf.getAddress(accCtx);
    }

    /**
     * Retrieves the NVMe-intern port directory index
     *
     * @param ipAddr    LsIpAddress, can be IPv4 or IPv6
     * @param extCmd    ExtCmd for executing commands
     * @return          String port index
     */
    private String getPortIdx(LsIpAddress ipAddr, ExtCmd extCmd)
        throws StorageException, IOException, ChildProcessTimeoutException
    {
        errorReporter.logDebug("NVMe: retrieving port directory index of IP address: " + ipAddr.getAddress());

        OutputData output = extCmd.exec(
            "/bin/bash", "-c", "grep -r -H --color=never " + ipAddr.getAddress() + " " + NVME_PORTS_PATH
        );

        String portIdx;
        if (output.exitCode == 0)
        {
            String grepPortIdx = new String(output.stdoutData);
            portIdx = grepPortIdx.substring(
                NVME_PORTS_PATH.length(),
                grepPortIdx.indexOf(File.separator, NVME_PORTS_PATH.length() + 1)
            );
        }
        else if (output.exitCode == 1)
        {
            portIdx = null;
        }
        else
        {
            throw new StorageException("Failed to look up port index!");
        }

        return portIdx;
    }

    public ExtCmdFactory getExtCmdFactory()
    {
        return extCmdFactory;
    }
}
