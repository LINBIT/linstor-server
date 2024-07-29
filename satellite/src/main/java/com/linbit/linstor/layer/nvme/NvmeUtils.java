package com.linbit.linstor.layer.nvme;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nonnull;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.DeviceProviderMapper;
import com.linbit.linstor.layer.storage.spdk.AbsSpdkProvider;
import com.linbit.linstor.layer.storage.spdk.SpdkCommands;
import com.linbit.linstor.layer.storage.spdk.utils.SpdkUtils;
import com.linbit.linstor.layer.storage.utils.DeviceUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.utils.ExceptionThrowingBiConsumer;

import static com.linbit.linstor.api.ApiConsts.KEY_PREF_NIC;
import static com.linbit.linstor.layer.storage.spdk.utils.SpdkUtils.SPDK_PATH_PREFIX;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Class for processing NvmeRscData
 *
 * @author Rainer Laschober
 *
 * @since v0.9.6
 */
@Singleton
public class NvmeUtils
{
    private static final long DFLT_WAIT_UNTIL_DEVICE_CREATED_TIMEOUT_IN_MS = 5000;

    public static final String NVME_SUBSYSTEM_PREFIX = "LS-NVMe_";
    public static final String STANDARD_NVME_SUBSYSTEM_PREFIX = "nqn.2018-02.linbit.linstor:"; // NQN format aligned to
                                                                                               // the NVMe Spec
    private static final String NVMET_PATH = "/sys/kernel/config/nvmet/";
    public static final String NVME_SUBSYSTEMS_PATH = NVMET_PATH + "subsystems/";
    private static final String NVME_PORTS_PATH = NVMET_PATH + "ports/";
    private static final String NVME_FABRICS_PATH = "/sys/devices/virtual/nvme-fabrics/ctl/nvme";

    private static final int IANA_DEFAULT_PORT = 4420;
    private static final int NVME_IDX_MAX_DIGITS = 5;
    private static final int NVME_GREP_SLEEP_MAX_WAIT_TIME = 50;
    private static final long NVME_GREP_SLEEP_INCREMENT = 200L;

    private final ExtCmdFactory extCmdFactory;
    private final ReadOnlyProps stltProps;
    private final ErrorReporter errorReporter;
    private final DeviceProviderMapper devProviderMapper;
    private final FileSystemWatch fsWatch;


    @Inject
    public NvmeUtils(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        @Named(LinStor.SATELLITE_PROPS) ReadOnlyProps stltPropsRef,
        DeviceProviderMapper devProviderMapperRef,
        FileSystemWatch fsWatchRef
    )
    {
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        stltProps = stltPropsRef;
        devProviderMapper = devProviderMapperRef;
        fsWatch = fsWatchRef;
    }

    /* compute methods */

    /**
     * Creates the necessary directories and files for the NVMe subsystem and namespaces
     * depending on the {@link NvmeRscData} and {@link NvmeVlmData}.
     *
     * @param nvmeRscData NvmeRscData object containing all needed information for this method
     * @param accCtx AccessContext needed to access properties and the IP address
     */
    public void createTargetRsc(NvmeRscData<Resource> nvmeRscData, AccessContext accCtx)
        throws StorageException
    {
        final String subsystemName = getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName();
        final String subsystemDirectory = NVME_SUBSYSTEMS_PATH + subsystemName;
        final Path subsystemPath = Paths.get(subsystemDirectory);

        errorReporter.logDebug(
            "NVMe: creating subsystem on target: " + NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
        );
        try
        {
            ResourceDefinition rscDfn = nvmeRscData.getAbsResource().getResourceDefinition();
            final PriorityProps nvmePrioProps = new PriorityProps(
                rscDfn.getProps(accCtx),
                rscDfn.getResourceGroup().getProps(accCtx),
                stltProps
            );
            if (nvmeRscData.isSpdk())
            {
                String transportType = nvmePrioProps.getProp(ApiConsts.KEY_TR_TYPE, ApiConsts.NAMESPC_NVME);
                if (transportType == null)
                {
                    transportType = "RDMA";
                }

                SpdkCommands<?> spdkCommands = getSpdkCommands(nvmeRscData);
                spdkCommands.ensureTransportExists(transportType);

                String port = nvmePrioProps.getProp(ApiConsts.KEY_PORT, ApiConsts.NAMESPC_NVME);
                if (port == null)
                {
                    port = Integer.toString(IANA_DEFAULT_PORT);
                }

                spdkCommands.nvmSubsystemCreate(subsystemName);

                LsIpAddress ipAddr = getIpAddr(nvmeRscData.getAbsResource(), accCtx);
                spdkCommands.nvmfSubsystemAddListener(
                    subsystemName,
                    transportType,
                    ipAddr.getAddress(),
                    ipAddr.getAddressType().toString(),
                    port
                );

                for (NvmeVlmData<Resource> nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
                {
                    // create namespace, set path to nvme device and enable namespace
                    createSpdkNamespace(nvmeVlmData, subsystemName);
                }

            }
            else
            {
                // create nvmet-rdma subsystem
                Files.createDirectories(subsystemPath);

                // allow any host to be connected
                Files.write(subsystemPath.resolve("attr_allow_any_host"), "1".getBytes());

                for (NvmeVlmData<Resource> nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
                {
                    // create namespace, set path to nvme device and enable namespace
                    createNamespace(nvmeVlmData, subsystemDirectory);
                }

                // get port directory or create it if the first subsystem is being added
                LsIpAddress ipAddr = getIpAddr(nvmeRscData.getAbsResource(), accCtx);
                String portIdx = getPortIdx(ipAddr);

                if (portIdx == null)
                {
                    errorReporter.logDebug("NVMe: creating new ports directory on target");

                    // get existing port directories and compute next available index
                    String[] portDirs = new File(NVME_PORTS_PATH).list();
                    if (portDirs.length == 0)
                    {
                        portIdx = "1";
                    }
                    else
                    {
                        Arrays.sort(portDirs);
                        portIdx = Integer.toString(
                            Integer.parseInt(portDirs[portDirs.length - 1].trim()) + 1
                        );
                    }

                    Path portsPath = Paths.get(NVME_PORTS_PATH + portIdx);
                    Files.createDirectories(portsPath);
                    Files.write(portsPath.resolve("addr_traddr"), ipAddr.getAddress().getBytes());

                    // set the transport type and port
                    String transportType = nvmePrioProps.getProp(ApiConsts.KEY_TR_TYPE, ApiConsts.NAMESPC_NVME);
                    if (transportType == null)
                    {
                        transportType = "rdma";
                    }
                    Files.write(portsPath.resolve("addr_trtype"), transportType.getBytes());

                    String port = nvmePrioProps.getProp(ApiConsts.KEY_PORT, ApiConsts.NAMESPC_NVME);
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
        catch (InvalidNameException | AccessDeniedException | InvalidKeyException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private SpdkCommands<?> getSpdkCommands(NvmeRscData<Resource> nvmeRscData)
    {
        SpdkData<Resource> spdkVlmChild = getSpdkChild(
            nvmeRscData.getVlmLayerObjects().values().iterator().next()
        );

        SpdkCommands<?> spdkCommands = ((AbsSpdkProvider<?>) devProviderMapper
            .getDeviceProviderByKind(spdkVlmChild.getProviderKind()))
                .getSpdkCommands();
        return spdkCommands;
    }

    /**
     * Reverses the operations executed by createTargetRsc(), thus deleting the data on the NVMe Target
     *
     * @param nvmeRscData NvmeRscData object containing all needed information for this method
     * @param accCtx AccessContext needed to access properties and the IP address
     */
    public void deleteTargetRsc(NvmeRscData<Resource> nvmeRscData, AccessContext accCtx)
        throws StorageException
    {
        final String subsystemName = getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName();
        final String subsystemDirectory = NVME_SUBSYSTEMS_PATH + subsystemName;

        errorReporter.logDebug(
            "NVMe: cleaning up target: " + NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
        );

        try
        {
            if (nvmeRscData.isSpdk())
            {
                SpdkCommands<?> spdkCommands = getSpdkCommands(nvmeRscData);
                spdkCommands.nvmfDeleteSubsystem(subsystemName);
            }
            else
            {
                // remove soft link
                String portIdx = getPortIdx(getIpAddr(nvmeRscData.getAbsResource(), accCtx));
                if (portIdx == null)
                {
                    throw new StorageException(
                        "No ports directory for NVMe subsystem '" + subsystemName + "' exists!"
                    );
                }

                if (!(new File(NVME_PORTS_PATH + portIdx + "/subsystems/" + subsystemName).delete()))
                {
                    throw new StorageException("Failed to remove symbolic link!");
                }

                // delete ports directory
                File subsysDir = new File(NVME_PORTS_PATH + portIdx + "/subsystems");
                if (!subsysDir.exists())
                {
                    if (!(new File(NVME_PORTS_PATH + portIdx).delete()))
                    {
                        throw new StorageException("Failed to delete ports directory!");
                    }
                }

                for (NvmeVlmData<Resource> nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
                {
                    // delete namespace
                    deleteNamespace(nvmeVlmData, subsystemDirectory);
                }

                // delete subsystem directory
                if (!(new File(subsystemDirectory).delete()))
                {
                    throw new StorageException("Failed to delete subsystem directory!");
                }
            }
            nvmeRscData.setExists(false);
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to delete NVMe target!", exc);
        }
        catch (InvalidNameException | AccessDeniedException | InvalidKeyException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Connects the NVMe Initiator to the Target after discovering available subsystems, given the subsystem name
     *
     * @param nvmeRscData
     *     NvmeRscData object containing all needed information for this method
     * @param accCtx
     *     AccessContext needed to access properties and Target resource
     *
     * @throws StorageException
     */
    public void connect(NvmeRscData<Resource> nvmeRscData, AccessContext accCtx) throws StorageException
    {
        try
        {
            connect(
                nvmeRscData,
                getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName(),
                getIpAddr(
                    // TODO: check on controller
                    nvmeRscData.getAbsResource().getResourceDefinition().getResource(
                        accCtx,
                        new NodeName(
                            nvmeRscData
                                .getAbsResource()
                                .getProps(accCtx)
                                .getProp(InternalApiConsts.PROP_NVME_TARGET_NODE_NAME)
                        )
                    ),
                    accCtx
                ).getAddress(),
                accCtx
            );
        }
        catch (InvalidKeyException | AccessDeniedException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public <VLM_DATA extends VlmProviderObject<Resource>, RSC_DATA extends AbsRscData<Resource, VLM_DATA>> void connect(
        RSC_DATA rscData,
        String subsystemName,
        String ipAddr,
        AccessContext accCtx
    )
        throws StorageException
    {
        try
        {
            errorReporter.logDebug("NVMe: connecting initiator to: " + subsystemName);

            if (subsystemName == null || subsystemName.trim().isEmpty())
            {
                throw new ImplementationError("Invalid (empty) subsystem name: '" + subsystemName + "'");
            }

            ResourceDefinition rscDfn = rscData.getAbsResource().getResourceDefinition();
            final PriorityProps nvmePrioProps = new PriorityProps(
                rscDfn.getProps(accCtx),
                rscDfn.getResourceGroup().getProps(accCtx),
                stltProps
            );

            String port = nvmePrioProps.getProp(ApiConsts.KEY_PORT, ApiConsts.NAMESPC_NVME);
            if (port == null)
            {
                port = Integer.toString(IANA_DEFAULT_PORT);
            }
            String transportType = nvmePrioProps.getProp(ApiConsts.KEY_TR_TYPE, ApiConsts.NAMESPC_NVME);
            if (transportType == null)
            {
                transportType = "rdma";
            }

            String nodeName = rscData.getAbsResource().getNode().getName().getDisplayName();
            List<String> subsystemNames = discover(subsystemName, transportType, ipAddr, port, nodeName);
            if (!subsystemNames.contains(subsystemName))
            {
                throw new StorageException("Failed to discover subsystem name '" + subsystemName + "'!");
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
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Disconnects the NVMe Initiator from the Target, given the subsystem name
     *
     * @param rscData
     *     NvmeRscData object containing all needed information for this method
     *
     * @throws StorageException
     */
    public void disconnect(NvmeRscData<Resource> nvmeRsc) throws StorageException
    {
        disconnect(
            nvmeRsc,
            getNvmeSubsystemPrefix(nvmeRsc) + nvmeRsc.getSuffixedResourceName(),
            NvmeRscData::setExists,
            NvmeVlmData::setExists,
            NvmeVlmData::getDevicePath
        );
    }

    public <VLM_DATA extends VlmProviderObject<Resource>, RSC_DATA extends AbsRscData<Resource, VLM_DATA>>
    void disconnect(
        RSC_DATA rscData,
        String subsystemName,
        BiConsumer<RSC_DATA, @Nonnull Boolean> setExistsRscFunc,
        ExceptionThrowingBiConsumer<VLM_DATA, @Nonnull Boolean, DatabaseException> setExistsVlmFunc,
        Function<VLM_DATA, String> getDevPathVlmFunc
    )
        throws StorageException
    {
        try
        {
            errorReporter.logDebug("NVMe: disconnecting initiator from: " + subsystemName);

            // workaround to prevent hanging `nvme disconnect`
            if (isAnyMounted(rscData, getDevPathVlmFunc))
            {
                throw new StorageException("Cannot disconnect mounted nvme-device.");
            }

            OutputData output = extCmdFactory.create().exec(
                "nvme",
                "disconnect",
                "--nqn=" + subsystemName
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to disconnect from NVMe target!");

            setDeepExists(rscData, setExistsRscFunc, setExistsVlmFunc, false);
        }
        catch (IOException | ChildProcessTimeoutException | DatabaseException exc)
        {
            throw new StorageException("Failed to disconnect from NVMe target!", exc);
        }
    }

    private <VLM_DATA extends VlmProviderObject<Resource>, RSC_DATA extends AbsRscData<Resource, VLM_DATA>>
    boolean isAnyMounted(
        RSC_DATA rscData,
        Function<VLM_DATA, String> getDevPathVlmFunc
    )
        throws ChildProcessTimeoutException, IOException
    {
        boolean mounted = false;
        StringBuilder grepArg = new StringBuilder();
        for (VLM_DATA vlmData : rscData.getVlmLayerObjects().values())
        {
            grepArg.append(getDevPathVlmFunc.apply(vlmData)).append("|");
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
     * @param nvmeRscData NvmeRscData object containing all needed information for this method
     *
     * @return boolean true if the subsystem directory exists and false otherwise
     *
     * @throws AccessDeniedException
     */
    public boolean isTargetConfigured(NvmeRscData<Resource> nvmeRscData) throws AccessDeniedException
    {
        final String subsystemName = getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName();

        errorReporter.logDebug("NVMe: checking if subsystem " + subsystemName + " exists.");

        boolean isConfigured = false;
        if (nvmeRscData.isSpdk())
        {
            try
            {
                isConfigured = SpdkUtils.checkTargetExists(getSpdkCommands(nvmeRscData), subsystemName);
            }
            catch (StorageException exc)
            {
                errorReporter.reportError(exc);
            }
        }
        else
        {
            isConfigured = Files.exists(Paths.get(NVME_SUBSYSTEMS_PATH).resolve(subsystemName));
        }

        return isConfigured;
    }

    /**
     * Queries NVMe-specific indices for the {@link NvmeRscData} and {@link NvmeVlmData}
     * and stores the result as the device path (for example '/dev/nvme2n1')
     *
     * @param nvmeRscData NvmeRscData object containing all needed information for this method
     * @param isWaiting boolean true if the external commands should be waited for in loop
     *
     * @return boolean true if the data was found and false otherwise
     */
    public boolean setDevicePaths(NvmeRscData<Resource> nvmeRscData, boolean isWaiting)
        throws StorageException
    {
        return setDevicePaths(
            isWaiting,
            nvmeRscData,
            getNvmeSubsystemPrefix(nvmeRscData) + nvmeRscData.getSuffixedResourceName(),
            // (rscData, exists) -> setExistsDeep(rscData, exists)
            NvmeRscData::setExists,
            NvmeVlmData::setExists,
            NvmeVlmData::setDevicePath
        );
    }

    public <VLM_DATA extends VlmProviderObject<Resource>, RSC_DATA extends AbsRscData<Resource, VLM_DATA>>
    boolean setDevicePaths(
        boolean isWaiting,
        RSC_DATA rscData,
        String subsystemName,
            BiConsumer<RSC_DATA, @Nonnull Boolean> setExistsRscFunc,
            ExceptionThrowingBiConsumer<VLM_DATA, @Nonnull Boolean, DatabaseException> setExistsVlmFunc,
        ExceptionThrowingBiConsumer<VLM_DATA, String, DatabaseException> setDevPathVlmFunc
    )
        throws StorageException
    {
        boolean success = true;
        try
        {
            errorReporter.logDebug("NVMe: trying to set device paths for: " + subsystemName);

            if (subsystemName == null || subsystemName.trim().isEmpty())
            {
                throw new ImplementationError("Subsystemname cannot be empty: '" + subsystemName + "'");
            }
            OutputData output = executeCmdAfterWaiting(
                isWaiting,
                "/bin/bash",
                "-c",
                "grep -H -r -w " + subsystemName + " " + NVME_FABRICS_PATH + "*/subsysnqn"
            );

            if (output == null)
            {
                success = false;
                setDeepExists(rscData, setExistsRscFunc, setExistsVlmFunc, false);
            }
            else
            {
                final int nvmeRscIdx = Integer.parseInt(
                    (new String(output.stdoutData))
                        .substring(NVME_FABRICS_PATH.length(), NVME_FABRICS_PATH.length() + NVME_IDX_MAX_DIGITS)
                        .split(File.separatorChar == '\\' ? "\\\\" : File.separator)[0]
                );
                final String nvmeFabricsVlmPath = NVME_FABRICS_PATH + nvmeRscIdx + "/nvme" + nvmeRscIdx;

                setExistsRscFunc.accept(rscData, true);

                for (VLM_DATA vlmData : rscData.getVlmLayerObjects().values())
                {
                    output = executeCmdAfterWaiting(
                        isWaiting,
                        "/bin/bash",
                        "-c",
                        "grep -H -r -w " + (vlmData.getVlmNr().getValue() + 1) + " " +
                            nvmeFabricsVlmPath + "*n*/nsid"

                    );
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
                        setExistsVlmFunc.accept(vlmData, false);
                        setDevPathVlmFunc.accept(vlmData, null);
                    }
                    else
                    {
                        String grepResult = new String(output.stdoutData);
                        // grepResult looks something like
                        // /sys/devices/virtual/nvme-fabrics/ctl/nvme0/nvme0c1n1/nsid:1
                        // either with cY or without

                        String[] nvmePathParts = grepResult.split(File.separatorChar == '\\' ? "\\\\" : File.separator);
                        // should now only contain "nvme0(c1)?n1"
                        String nvmeNamespacePart = nvmePathParts[nvmePathParts.length - 2];

                        final int nvmeVlmIdx = Integer.parseInt(
                            nvmeNamespacePart.substring(nvmeNamespacePart.lastIndexOf('n') + 1)
                        );

                        String devicePath = "/dev/nvme" + nvmeRscIdx + "n" + nvmeVlmIdx;
                        DeviceUtils.waitUntilDeviceVisible(
                            devicePath,
                            DFLT_WAIT_UNTIL_DEVICE_CREATED_TIMEOUT_IN_MS,
                            errorReporter,
                            fsWatch
                        );
                        setDevPathVlmFunc.accept(vlmData, devicePath);
                        setExistsVlmFunc.accept(vlmData, true);
                    }
                }
            }
        }
        catch (IOException | ChildProcessTimeoutException | InterruptedException | DatabaseException exc)
        {
            throw new StorageException("Failed to set NVMe device path!", exc);
        }

        return success;
    }

    private <VLM_DATA extends VlmProviderObject<Resource>, RSC_DATA extends AbsRscData<Resource, VLM_DATA>>
    void setDeepExists(
        RSC_DATA rscData,
        BiConsumer<RSC_DATA, Boolean> setExistsRscFunc,
            ExceptionThrowingBiConsumer<VLM_DATA, Boolean, DatabaseException> setExistsVlmFunc,
        boolean exists
        ) throws DatabaseException
    {
        setExistsRscFunc.accept(rscData, exists);
        for (VLM_DATA vlmData : rscData.getVlmLayerObjects().values())
        {
            setExistsVlmFunc.accept(vlmData, exists);
        }
    }

    /**
     * Sets the exists flag on the given {@link NvmeRscData} as well as on all its child-{@link NvmeVlmData}
     * objects
     *
     * @param nvmeRscDataRef
     * @param existsRef
     *
     * @throws DatabaseException
     */
    private void setExistsDeep(NvmeRscData<Resource> nvmeRscDataRef, boolean existsRef) throws DatabaseException
    {
        nvmeRscDataRef.setExists(existsRef);
        for (NvmeVlmData<Resource> nvmeVlmData : nvmeRscDataRef.getVlmLayerObjects().values())
        {
            nvmeVlmData.setExists(existsRef);
        }
    }

    /**
     * Creates a new directory for the given file path, sets the appropriate backing device and enables the namespace
     *
     * @param nvmeVlmData
     *     nvmeVlm object containing information for path to the new namespace
     *
     * @throws DatabaseException
     */
    public void createNamespace(NvmeVlmData<Resource> nvmeVlmData, String subsystemDirectory)
        throws IOException, DatabaseException
    {
        final Path namespacePath = Paths.get(
            subsystemDirectory + "/namespaces/" + (nvmeVlmData.getVlmNr().getValue() + 1)
        );
        if (!Files.exists(namespacePath))
        {
            byte[] backingDevice = nvmeVlmData.getDataDevice().getBytes();
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
     * @param nvmeVlmData
     *     nvmeVlm object containing information for path to the new namespace
     * @param subsystemDirectory
     *
     * @throws DatabaseException
     */
    public void deleteNamespace(NvmeVlmData<Resource> nvmeVlmData, String subsystemDirectory)
        throws IOException, ChildProcessTimeoutException, StorageException, DatabaseException
    {
        final int namespaceNr = nvmeVlmData.getVlmNr().getValue() + 1;
        final Path namespacePath = Paths.get(subsystemDirectory + "/namespaces/" + namespaceNr);

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
     * @param nvmeVlmData
     *     nvmeVlm object containing information for path to the new namespace
     * @param subsystemName
     *     String containing NVMe subsystem name
     *
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    public void createSpdkNamespace(NvmeVlmData<Resource> nvmeVlmData, String subsystemName)
        throws IOException, StorageException, ChildProcessTimeoutException, AccessDeniedException, DatabaseException
    {
        SpdkCommands<?> spdkCommands = getSpdkCommands(nvmeVlmData.getRscLayerObject());
        if (!SpdkUtils.checkNamespaceExists(
            spdkCommands,
            subsystemName,
            nvmeVlmData.getVlmNr().getValue() + 1
        ))
        {
            String spdkPath = getSpdkChild(nvmeVlmData).getSpdkPath();

            errorReporter.logDebug("NVMe: exposing device: " + spdkPath);

            spdkCommands.nvmfSubsystemAddNs(subsystemName, spdkPath.split(SPDK_PATH_PREFIX)[1]);
        }
        nvmeVlmData.setExists(true);
    }

    private SpdkData<Resource> getSpdkChild(NvmeVlmData<Resource> nvmeVlmDataRef)
    {
        VlmProviderObject<Resource> child = nvmeVlmDataRef.getSingleChild();
        if (!(child instanceof SpdkData))
        {
            throw new ImplementationError("Unexpected type between NVMe and SPDK: " + child.getLayerKind());
        }
        return (SpdkData<Resource>) child;
    }

    /**
     * Disables the namespace in a SPDK NVMe subsystem
     *
     * @param nvmeVlmData
     *     nvmeVlm object containing information for path to the new namespace
     * @param subsystemName
     *
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    public void deleteSpdkNamespace(NvmeVlmData<Resource> nvmeVlmData, String subsystemName)
        throws IOException, ChildProcessTimeoutException, StorageException, AccessDeniedException, DatabaseException
    {
        final int namespaceNr = nvmeVlmData.getVlmNr().getValue() + 1;

        SpdkCommands<?> spdkCommands = getSpdkCommands(nvmeVlmData.getRscLayerObject());
        if (!SpdkUtils.checkNamespaceExists(
            spdkCommands,
            subsystemName,
            namespaceNr
        ))
        {
            errorReporter.logDebug("NVMe: deleting namespace: " + subsystemName);

            spdkCommands.nvmfSubsystemRemoveNamespace(subsystemName, namespaceNr);
        }
        nvmeVlmData.setExists(false);
    }

    /**
     * Returns NVMe subsystem prefix depending on {@param NvmeRscData}
     *
     * @param nvmeRscData NvmeRscData, Resource object containing the needed flag for this method
     *
     * @return String with NVME subsystem prefix
     */
    public static String getNvmeSubsystemPrefix(NvmeRscData<?> nvmeRscData)
    {
        return nvmeRscData.isSpdk() ? STANDARD_NVME_SUBSYSTEM_PREFIX : NVME_SUBSYSTEM_PREFIX;
    }

    /* helper methods */

    /**
     * Executes the given command immediately or waits for its completion, depending on {@param isWaiting}
     *
     * @param isWaiting boolean true if the external commands should be waited for in loop
     * @param command String... command being executed
     *
     * @return OutputData of the executed command or null if something went wrong
     */
    private @Nullable OutputData executeCmdAfterWaiting(boolean isWaiting, String... command)
        throws IOException, ChildProcessTimeoutException, InterruptedException
    {
        int tries = isWaiting ? 0 : NVME_GREP_SLEEP_MAX_WAIT_TIME - 1;
        OutputData output;
        boolean extCmdSuccess;
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
     * @param nvmeRscData NvmeRscData, object containing needed NVMe subsystem prefix
     * @param transportType String, only RDMA featured at the moment
     * @param ipAddr String, can be IPv4 or IPv6
     * @param port String, default: 4420
     *
     * @return List<String> of discovered subsystem names
     */
    private List<String> discover(
        String subsystemName,
        String transportType,
        String ipAddr,
        String port,
        String nodeName
    )
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
                    int idx = outputLine.indexOf(subsystemName);
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
     * @param rsc Resource object containing all needed information for this method
     * @param accCtx AccessContext needed to access properties and the net interface
     *
     * @return {@link LsIpAddress} of the resource's net interface
     */
    private LsIpAddress getIpAddr(Resource rsc, AccessContext accCtx)
        throws StorageException, InvalidNameException, AccessDeniedException, InvalidKeyException
    {
        LsIpAddress ipAddr;
        if (rsc.getNode().getNodeType(accCtx).equals(Node.Type.REMOTE_SPDK))
        {
            ipAddr = getIpAddrFromSpecialStlt(rsc, accCtx);
        }
        else
        {
            ipAddr = getIpAddrFromNormalStlt(rsc, accCtx);
        }
        return ipAddr;
    }

    private LsIpAddress getIpAddrFromSpecialStlt(Resource rscRef, AccessContext accCtxRef)
        throws StorageException, InvalidKeyException, AccessDeniedException
    {
        try
        {
            // TODO: should be more general
            return new LsIpAddress(
                rscRef.getNode().getProps(accCtxRef).getProp(
                    ApiConsts.KEY_STOR_POOL_REMOTE_SPDK_API_HOST,
                    ApiConsts.NAMESPC_STORAGE_DRIVER
                )
            );
        }
        catch (InvalidIpAddressException exc)
        {
            throw new StorageException("Invalid IP address", exc);
        }
    }

    private LsIpAddress getIpAddrFromNormalStlt(Resource rsc, AccessContext accCtx)
        throws AccessDeniedException, StorageException, InvalidNameException
    {
        PriorityProps prioProps = new PriorityProps();
        Iterator<Volume> iterateVolumes = rsc.iterateVolumes();

        Set<StorPool> storPools = new TreeSet<>();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();
            prioProps.addProps(vlm.getProps(accCtx));

            storPools.addAll(LayerVlmUtils.getStorPoolSet(vlm, accCtx, true));
        }
        for (StorPool storPool : storPools)
        {
            prioProps.addProps(storPool.getProps(accCtx));
        }
        prioProps.addProps(rsc.getProps(accCtx));
        prioProps.addProps(rsc.getNode().getProps(accCtx));
        prioProps.addProps(stltProps);

        String netIfName = prioProps.getProp(KEY_PREF_NIC, ApiConsts.NAMESPC_NVME);

        NetInterface netIf;
        if (netIfName == null)
        {
            Iterator<NetInterface> iterateNetInterfaces = rsc.getNode().iterateNetInterfaces(accCtx);
            if (iterateNetInterfaces.hasNext())
            {
                netIf = iterateNetInterfaces.next();
            }
            else
            {
                throw new StorageException("No network interface defined for node " + rsc.getNode().getName());
            }
        }
        else
        {
            errorReporter.logDebug("NVMe: querying net interface " + netIfName + "for IP address.");

            Node rscNode = rsc.getNode();
            netIf = rscNode.getNetInterface(accCtx, new NetInterfaceName(netIfName));
            if (netIf == null)
            {
                throw new StorageException(
                    "The preferred network interface '" + netIfName + "' of node '" +
                        rscNode.getName() + "' does not exist!"
                ); // TODO: call checkPrefNic() when rsc.getAssignedNode().getNetInterface(accCtx, new
                   // NetInterfaceName(netIfName)) is called
            }
        }

        return netIf.getAddress(accCtx);
    }

    /**
     * Retrieves the NVMe-intern port directory index
     *
     * @param ipAddr LsIpAddress, can be IPv4 or IPv6
     *
     * @return String port index
     */
    private @Nullable String getPortIdx(LsIpAddress ipAddr)
        throws StorageException, IOException, ChildProcessTimeoutException
    {
        errorReporter.logDebug("NVMe: retrieving port directory index of IP address: " + ipAddr.getAddress());

        OutputData output = extCmdFactory.create().exec(
            "/bin/bash",
            "-c",
            "grep -r -H --color=never " + ipAddr.getAddress() + " " + NVME_PORTS_PATH
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
     * @param rscData
     *     resource data (of any kind, might be NvmeRscData, but also others) object containing all needed information
     *     for this method
     *
     * @return boolean true if the resource belongs to SPDK and false otherwise
     */
    public boolean isSpdkResource(AbsRscLayerObject<Resource> rscData)
    {
        Set<AbsRscLayerObject<Resource>> storageResources = LayerRscUtils.getRscDataByLayer(
            rscData,
            DeviceLayerKind.STORAGE
        );

        boolean isSpdk = true;
        for (AbsRscLayerObject<Resource> storageRsc : storageResources)
        {
            for (VlmProviderObject<Resource> vlmData : storageRsc.getVlmLayerObjects().values())
            {
                if (!(vlmData instanceof SpdkData))
                {
                    isSpdk = false;
                    break;
                }
            }
        }
        return isSpdk;
    }

    /**
     * Returns target resource associated with initiator resource
     *
     * @param nvmeRscData NvmeRscData object containing information needed for this method
     * @param accCtx AccessContext needed to access properties
     *
     * @return Resource target resource
     */
    public Resource getTargetResource(NvmeRscData<Resource> nvmeRscData, AccessContext accCtx)
        throws AccessDeniedException, StorageException
    {
        return getTargetResource(nvmeRscData.getAbsResource(), accCtx);
    }

    public Resource getTargetResource(Resource initiatorRsc, AccessContext accCtx)
        throws AccessDeniedException, StorageException
    {
        Optional<Resource> targetRscOpt = initiatorRsc
            .getResourceDefinition()
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
