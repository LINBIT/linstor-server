package com.linbit.linstor.storage.layer.adapter.nvme;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.SpdkCommands;
import com.linbit.linstor.storage.utils.SpdkUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import static com.linbit.linstor.api.ApiConsts.KEY_PREF_NIC;
import static com.linbit.linstor.storage.utils.SpdkCommands.SPDK_RPC_SCRIPT;
import static com.linbit.linstor.storage.utils.SpdkUtils.SPDK_PATH_PREFIX;

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
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

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
    public static final String STANDARD_NVME_SUBSYSTEM_PREFIX = "nqn.2018-02.linbit.linstor:"; // NQN format aligned to the NVMe Spec
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
        final String subsystemName = getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName();
        final String subsystemDirectory = NVME_SUBSYSTEMS_PATH + subsystemName;
        final Path subsystemPath = Paths.get(subsystemDirectory);

        errorReporter.logDebug(
                "NVMe: creating subsystem on target: " + subsystemName
        );

        try
        {
            if (nvmeRscData.isSpdk())
            {
                SpdkCommands.createTransport(extCmdFactory.create(),"RDMA");

                ResourceDefinition rscDfn = nvmeRscData.getResource().getDefinition();
                final PriorityProps nvmePrioProps = new PriorityProps(
                        rscDfn.getProps(accCtx),
                        rscDfn.getResourceGroup().getProps(accCtx),
                        stltProps
                );

                String port = nvmePrioProps.getProp(ApiConsts.KEY_PORT);
                if (port == null)
                {
                    port = Integer.toString(IANA_DEFAULT_PORT);
                };

                OutputData output = extCmdFactory.create().exec(
                        SPDK_RPC_SCRIPT,
                        "nvmf_subsystem_create",
                        subsystemName,
                        "--allow-any-host"
                );
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to create subsystem!");

                output = extCmdFactory.create().exec(
                        SPDK_RPC_SCRIPT,
                        "nvmf_subsystem_add_listener",
                        subsystemName,
                        "-t",
                        "RDMA",
                        "-a",
                        getIpAddr(nvmeRscData.getResource(), accCtx).getAddress(),
                        "-f",
                        getIpAddr(nvmeRscData.getResource(), accCtx).getAddressType().toString(),
                        "-s",
                        port
                    );
                    ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to add listener to subsystem!");

                    for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
                    {
                        // create namespace, set path to nvme device and enable namespace
                        createSpdkNamespace(nvmeVlmData, subsystemName);
                    }
            }
            else
            {

            ResourceDefinition rscDfn = nvmeRscData.getResource().getDefinition();
            final PriorityProps nvmePrioProps = new PriorityProps(
                rscDfn.getProps(accCtx),
                rscDfn.getResourceGroup().getProps(accCtx),
                stltProps
            );

            // create nvmet-rdma subsystem
            Files.createDirectories(subsystemPath);

            // allow any host to be connected
            Files.write(subsystemPath.resolve("attr_allow_any_host"), "1".getBytes());

            for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
            {
                // create namespace, set path to nvme device and enable namespace
                createNamespace(nvmeVlmData, subsystemDirectory);
            }

            // get port directory or create it if the first subsystem is being added
            LsIpAddress ipAddr = getIpAddr(nvmeRscData.getResource(), accCtx);
            String portIdx = getPortIdx(ipAddr);

            if (portIdx == null)
            {
                errorReporter.logDebug("NVMe: creating new ports directory on target");

                // get existing port directories and compute next available index
                OutputData output = extCmdFactory.create()
                    .exec("/bin/bash", "-c", "ls -m --color=never " + NVME_PORTS_PATH);
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
                extCmdFactory.create().exec(
                    "ln",
                    "-s",
                    subsystemDirectory,
                    NVME_PORTS_PATH + portIdx + "/subsystems/" + subsystemName
                    ),
                StorageException::new,
                "Failed to create symbolic link!"
            );
            }
            nvmeRscData.setExists(true);
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
        final String subsystemName = getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName();
        final String subsystemDirectory = NVME_SUBSYSTEMS_PATH + subsystemName;

        errorReporter.logDebug(
                "NVMe: cleaning up target: " + subsystemName
        );

        try {
            if (nvmeRscData.isSpdk())
            {
                OutputData output = extCmdFactory.create().exec(
                        SPDK_RPC_SCRIPT,
                        "delete_nvmf_subsystem",
                        subsystemName
                );
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete subsystem!");
            }
            else
            {

            // remove soft link
            String portIdx = getPortIdx(getIpAddr(nvmeRscData.getResource(), accCtx));
            if (portIdx == null)
            {
                throw new StorageException(
                    "No ports directory for NVMe subsystem \'" + subsystemName + "\' exists!"
                );
            }

            OutputData output = extCmdFactory.create().exec(
                "rm", NVME_PORTS_PATH + portIdx + "/subsystems/" + subsystemName
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to remove symbolic link!");

            // delete ports directory
            output = extCmdFactory.create().exec(
                "/bin/bash", "-c", "ls -m --color=never " + NVME_PORTS_PATH + portIdx + "/subsystems"
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to list files!");

            if (new String(output.stdoutData).trim().isEmpty())
            {
                output = extCmdFactory.create().exec("rmdir", NVME_PORTS_PATH + portIdx);
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete ports directory!");
            }

            for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
            {
                // delete namespace
                deleteNamespace(nvmeVlmData, subsystemDirectory);
            }

            // delete subsystem directory
            output = extCmdFactory.create().exec("rmdir", subsystemDirectory);
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete subsystem directory!");
            }
            nvmeRscData.setExists(false);
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
        final String subsystemName = getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName();

        try
        {
            errorReporter.logDebug(
                "NVMe: connecting initiator to: " + subsystemName
            );

            ResourceDefinition rscDfn = nvmeRscData.getResource().getDefinition();
            final PriorityProps nvmePrioProps = new PriorityProps(
                rscDfn.getProps(accCtx),
                rscDfn.getResourceGroup().getProps(accCtx),
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
                rscDfn.streamResource(accCtx).filter(
                    rsc -> !rsc.equals(nvmeRscData.getResource())
                ).findFirst().orElseThrow(() -> new StorageException("Target resource not found!")),
                accCtx
            ).getAddress();

            String nodeName = nvmeRscData.getResource().getAssignedNode().getName().getDisplayName();
            List<String> subsystemNames = discover(nvmeRscData, transportType, ipAddr, port, nodeName);
            if (!subsystemNames.contains(subsystemName))
            {
                throw new StorageException("Failed to discover subsystem name \'" + subsystemName + "\'!");
            }

            OutputData output = extCmdFactory.create().exec(
                "nvme",
                "connect",
                "--transport=" + transportType,
                "--nqn=" + subsystemName,
                "--traddr=" + ipAddr,
                "--trsvcid=" + port,
                "--hostnqn=" + nodeName
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
        final String subsystemName = getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName();

        try
        {
            errorReporter.logDebug(
                "NVMe: disconnecting initiator from: " + subsystemName
            );

            // workaround to prevent hanging `nvme disconnect`
            if (isAnyMounted(nvmeRscData))
            {
                throw new StorageException("Cannot disconnect mounted nvme-device.");
            }

            OutputData output = extCmdFactory.create().exec(
                "nvme",
                "disconnect",
                "--nqn=" + subsystemName
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to disconnect from NVMe target!");

            setExistsDeep(nvmeRscData, false);
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to disconnect from NVMe target!", exc);
        }
    }

    private boolean isAnyMounted(NvmeRscData nvmeRscDataRef)
        throws ChildProcessTimeoutException, IOException, StorageException
    {
        boolean mounted = false;
        StringBuilder grepArg = new StringBuilder();
        for (NvmeVlmData vlm : nvmeRscDataRef.getVlmLayerObjects().values())
        {
            grepArg.append(vlm.getDevicePath()).append("|");
        }
        if (grepArg.length() > 0)
        {
            grepArg.setLength(grepArg.length() - 1);
            OutputData output = extCmdFactory.create().exec("/bin/bash", "-c", "mount | grep -Ew " + grepArg);
            String outStr = new String(output.stdoutData);
            mounted = !outStr.trim().isEmpty();
        }
        else
        {
            errorReporter.logTrace("grepArg empty, skipping mount-check");
        }
        return mounted;
    }

    /**
     * Checks whether the specified subsystem directory exists
     *
     * @param nvmeRscData   NvmeRscData object containing all needed information for this method
     * @return              boolean true if the subsystem directory exists and false otherwise
     */
    public boolean isTargetConfigured(NvmeRscData nvmeRscData)
    {
        final String subsystemName = getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName();

        errorReporter.logDebug(
            "NVMe: checking if subsystem " + subsystemName + " exists."
        );

        if (nvmeRscData.isSpdk())
        {
            try {
                return SpdkUtils.checkTargetExists(extCmdFactory.create(),subsystemName);

            } catch (StorageException e) {
                e.printStackTrace();
                return false;
            }
        }

        return Files.exists(Paths.get(NVME_SUBSYSTEMS_PATH).resolve(subsystemName)
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
        final String subsystemName = getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName();

        try
        {
            errorReporter.logDebug("NVMe: trying to set device paths for: " + subsystemName);

            OutputData output = executeCmdAfterWaiting(
                isWaiting,
                "/bin/bash", "-c", "grep -H -r -w " + subsystemName + " " + NVME_FABRICS_PATH + "*/subsysnqn"
            );

            if (output == null)
            {
                success = false;
                setExistsDeep(nvmeRscData, false);
            }
            else
            {
                final int nvmeRscIdx = Integer.parseInt(
                    (new String(output.stdoutData))
                        .substring(NVME_FABRICS_PATH.length(), NVME_FABRICS_PATH.length() + NVME_IDX_MAX_DIGITS)
                        .split(File.separator)[0]
                );
                final String nvmeFabricsVlmPath = NVME_FABRICS_PATH + nvmeRscIdx + "/nvme" + nvmeRscIdx;

                nvmeRscData.setExists(true);

                for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
                {
                    output = executeCmdAfterWaiting(isWaiting,
                        "/bin/bash", "-c",
                        "grep -H -r -w " + (nvmeVlmData.getVlmNr().getValue() + 1) + " " +
                            nvmeFabricsVlmPath + "*n*/nsid");
                    /*
                     * device path might be
                     * .../nvmeX/nvmeXcYnZ/..
                     * but also
                     * .../nvmeX/nvmeXnY/..
                     *
                     */

                    if (output == null)
                    {
                        success = false;
                        nvmeVlmData.setExists(false);
                    }
                    else
                    {
                        String grepResult = new String(output.stdoutData);
                        // grepResult looks something like
                        // /sys/devices/virtual/nvme-fabrics/ctl/nvme0/nvme0c1n1/nsid:1
                        // either with cY or without

                        String[] nvmePathParts = grepResult.split(File.separator);
                        // should now only contain "nvme0(c1)?n1"
                        String nvmeNamespacePart = nvmePathParts[nvmePathParts.length - 2];

                        final int nvmeVlmIdx = Integer.parseInt(
                            nvmeNamespacePart.substring(nvmeNamespacePart.lastIndexOf('n') + 1)
                        );

                        nvmeVlmData.setDevicePath("/dev/nvme" + nvmeRscIdx + "n" + nvmeVlmIdx);
                        nvmeVlmData.setExists(true);
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
     * Sets the exists flag on the given {@link NvmeRscData} as well as on all its child-{@link NvmeVlmData}
     * objects
     * @param nvmeRscDataRef
     * @param existsRef
     */
    private void setExistsDeep(NvmeRscData nvmeRscDataRef, boolean existsRef)
    {
        nvmeRscDataRef.setExists(existsRef);
        for (NvmeVlmData nvmeVlmData : nvmeRscDataRef.getVlmLayerObjects().values())
        {
            nvmeVlmData.setExists(existsRef);
        }
    }


    /**
     * Creates a new directory for the given file path, sets the appropriate backing device and enables the namespace
     *
     * @param nvmeVlmData nvmeVlm object containing information for path to the new namespace
     */
    public void createNamespace(NvmeVlmData nvmeVlmData, String subsystemDirectory)
        throws IOException
    {
        final Path namespacePath = Paths.get(
            subsystemDirectory + "/namespaces/" + (nvmeVlmData.getVlmNr().getValue() + 1)
        );
        if (!Files.exists(namespacePath))
        {
            byte[] backingDevice = nvmeVlmData.getBackingDevice().getBytes();
            errorReporter.logDebug("NVMe: creating namespace: " + namespacePath.getFileName());
            Files.createDirectories(namespacePath);
            errorReporter.logDebug("NVMe: exposing device: " + new String(backingDevice));
            Files.write(namespacePath.resolve("device_path"), backingDevice);
            Files.write(namespacePath.resolve("enable"), "1".getBytes());
        }
        nvmeVlmData.setExists(true);
    }

    /**
     * Disables the namespace at the given file path and deletes the directory
     *
     * @param nvmeVlmData nvmeVlm object containing information for path to the new namespace
     * @param subsystemDirectory
     */
    public void deleteNamespace(NvmeVlmData nvmeVlmData, String subsystemDirectory)
        throws IOException, ChildProcessTimeoutException, StorageException
    {
        final int namespaceNr = nvmeVlmData.getVlmNr().getValue() + 1;
        final Path namespacePath = Paths.get(
            subsystemDirectory + "/namespaces/" + namespaceNr
        );

        if (Files.exists(namespacePath))
        {
            errorReporter.logDebug("NVMe: deleting namespace: " + namespacePath.getFileName());
            Files.write(namespacePath.resolve("enable"), "0".getBytes());
            OutputData output = extCmdFactory.create().exec("rmdir", namespacePath.toString());
            ExtCmdUtils.checkExitCode(
                output,
                StorageException::new,
                "Failed to delete namespace directory!"
            );
        }
        nvmeVlmData.setExists(false);
    }


    /**
     * Creates a new namespace in a SPCK NVMe subsystem
     *
     * @param nvmeVlmData nvmeVlm object containing information for path to the new namespace
     * @param subsystemName String containing NVMe subsystem name
     */
    public void createSpdkNamespace(NvmeVlmData nvmeVlmData, String subsystemName)
            throws IOException, StorageException, ChildProcessTimeoutException
    {
        if (!SpdkUtils.checkNamespaceExists(extCmdFactory.create(), subsystemName, nvmeVlmData.getVlmNr().getValue() + 1)) {
            byte[] backingDevice = nvmeVlmData.getBackingDevice().getBytes();
            String spdkPath = new String(backingDevice);

            errorReporter.logDebug("NVMe: exposing device: " + new String(backingDevice));

            OutputData output = extCmdFactory.create().exec(
                    SPDK_RPC_SCRIPT,
                    "nvmf_subsystem_add_ns",
                    subsystemName,
                    spdkPath.split(SPDK_PATH_PREFIX)[1]
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to create namespace!");
        }
        nvmeVlmData.setExists(true);
    }

    /**
     * Disables the namespace in a SPDK NVMe subsystem
     *
     * @param nvmeVlmData nvmeVlm object containing information for path to the new namespace
     * @param subsystemName
     */
    public void deleteSpdkNamespace(NvmeVlmData nvmeVlmData, String subsystemName)
            throws IOException, ChildProcessTimeoutException, StorageException
    {
        final int namespaceNr = nvmeVlmData.getVlmNr().getValue() + 1;

        if (!SpdkUtils.checkNamespaceExists(extCmdFactory.create(), subsystemName, namespaceNr))
        {
            errorReporter.logDebug("NVMe: deleting namespace: " + subsystemName);
            OutputData output = extCmdFactory.create().exec(
                    SPDK_RPC_SCRIPT,
                    "nvmf_subsystem_remove_ns",
                    subsystemName,
                    String.valueOf(namespaceNr)
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete namespace!");
        }
        nvmeVlmData.setExists(false);
    }

    /**
     * Returns NVMe subsystem prefix depending on {@param NvmeRscData}
     *
     * @param nvmeRscData NvmeRscData, Resource object containing the needed flag for this method
     * @return          String with NVME subsystem prefix
     */
    public static String getNvmeSubsystemPrefix(NvmeRscData nvmeRscData)
    {
        if(nvmeRscData.isSpdk())
        {
            return STANDARD_NVME_SUBSYSTEM_PREFIX;
        }

        return NVME_SUBSYSTEM_PREFIX;
    }


    /* helper methods */

    /**
     * Executes the given command immediately or waits for its completion, depending on {@param isWaiting}
     *
     * @param isWaiting boolean true if the external commands should be waited for in loop
     * @param command   String... command being executed
     * @return          OutputData of the executed command or null if something went wrong
     */
    private OutputData executeCmdAfterWaiting(boolean isWaiting, String... command)
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
            output = extCmdFactory.create().exec(command);
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
     * @param nvmeRscData   NvmeRscData, object containing needed NVMe subsystem prefix
     * @param transportType String, only RDMA featured at the moment
     * @param ipAddr        String, can be IPv4 or IPv6
     * @param port          String, default: 4420
     * @return              List<String> of discovered subsystem names
     */
    private List<String> discover(NvmeRscData nvmeRscData, String transportType, String ipAddr, String port, String nodeName)
        throws StorageException
    {
        List<String> subsystemNames = new ArrayList<>();

        try
        {
            errorReporter.logDebug("NVMe: discovering target subsystems.");

            OutputData output = extCmdFactory.create().exec(
                "nvme",
                "discover",
                "--transport=" + transportType,
                "--traddr=" + ipAddr,
                "--trsvcid=" + port,
                "--hostnqn=" + nodeName
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to discover NVMe subsystems!");

            for (String outputLine : (new String(output.stdoutData)).split("\n"))
            {
                if (outputLine.contains("subnqn:"))
                {
                    int idx = outputLine.indexOf(getNvmeSubsystemPrefix(nvmeRscData));
                    if (idx >= 0)
                    {
                        subsystemNames.add(outputLine.substring(idx));
                    }
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

        Set<StorPool> storPools = new TreeSet<>();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();
            prioProps.addProps(vlm.getProps(accCtx));

            storPools.addAll(LayerVlmUtils.getStorPoolSet(vlm, accCtx));
        }
        for (StorPool storPool : storPools)
        {
            prioProps.addProps(storPool.getProps(accCtx));
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
     * @return          String port index
     */
    private String getPortIdx(LsIpAddress ipAddr)
        throws StorageException, IOException, ChildProcessTimeoutException
    {
        errorReporter.logDebug("NVMe: retrieving port directory index of IP address: " + ipAddr.getAddress());

        OutputData output = extCmdFactory.create().exec(
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

    /**
     * Checks whether the specified resource belongs to SPDK layer
     *
     * @param nvmeRscData   NvmeRscData object containing all needed information for this method
     * @return              boolean true if the resource belongs to SPDK and false otherwise
     */
    public boolean isSpdkResource(RscLayerObject nvmeRscData)
    {
        Set<RscLayerObject> storageResources = LayerRscUtils.getRscDataByProvider(
                nvmeRscData,
                DeviceLayerKind.STORAGE
        );

        for (RscLayerObject storageRsc : storageResources)
        {
            for (VlmProviderObject vlmData : storageRsc.getVlmLayerObjects().values())
            {
                if (!(vlmData instanceof SpdkData))
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns target resource associated with initiator resource
     *
     * @param nvmeRscData   NvmeRscData object containing information needed for this method
     * @param accCtx        AccessContext needed to access properties
     * @return              Resource target resource
     */
    public Resource getTargetResource(RscLayerObject nvmeRscData, AccessContext accCtx)
            throws AccessDeniedException, StorageException
    {
        Optional<Resource> targetRscOpt = nvmeRscData
                .getResource()
                .getDefinition()
                .streamResource(accCtx).filter(
                        rsc ->
                        {
                            try
                            {
                                return rsc.getStateFlags().isUnset(accCtx, Resource.Flags.DISKLESS);
                            }
                            catch (AccessDeniedException exc)
                            {
                                throw new ImplementationError(exc);
                            }
                        }
                ).findFirst();
        if (!targetRscOpt.isPresent())
        {
            throw new StorageException("Missing NVMe Target resource associated with NVMe Initiator resource!");
        }
        return targetRscOpt.get();
    }

    public ExtCmdFactory getExtCmdFactory()
    {
        return extCmdFactory;
    }
}
