package com.linbit.linstor.storage.layer.adapter.nvme;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;

import static com.linbit.linstor.api.ApiConsts.DEFAULT_NETIF;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class NvmeUtils
{
    private static final String NVME_SUBSYSTEM_PREFIX = "LS-NVMe_";
    private static final String NVMET_PATH = "/sys/kernel/config/nvmet";
    private static final String NVME_SUBSYSTEMS_PATH = NVMET_PATH + "/subsystems/";
    private static final String NVME_PORTS_PATH = NVMET_PATH + "/ports/";
    private final Path portsPath = Paths.get(NVME_PORTS_PATH);

    private static final int IANA_DEFAULT_PORT = 4420;

    private final ExtCmdFactory extCmdFactory;
    private final Props stltProps;

    @Inject
    public NvmeUtils(ExtCmdFactory extCmdFactoryRef, @Named(LinStor.SATELLITE_PROPS) Props stltPropsRef)
    {
        extCmdFactory = extCmdFactoryRef;
        stltProps = stltPropsRef;
    }

    /* compute methods */

    public void configureTarget(NvmeRscData nvmeRscData, AccessContext accCtx)
        throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();
        final String subsystemName = NVME_SUBSYSTEM_PREFIX + nvmeRscData.getResourceName();
        final String subsystemDirectory = NVME_SUBSYSTEMS_PATH + subsystemName;
        final Path subsystemPath = Paths.get(subsystemDirectory);

        try
        {
            final PriorityProps nvmePrioProps = new PriorityProps(
                nvmeRscData.getResource().getDefinition().getProps(accCtx),
                stltProps
            );

            for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
            {
                final Path namespacePath = Paths.get(
                    subsystemDirectory + "/namespaces/" + nvmeVlmData.getVlmNr().value + 1
                );

                // create nvmet-rdma subsystem
                Files.createDirectories(subsystemPath);

                // allow any host to be connected
                Files.write(subsystemPath.resolve("attr_allow_any_host"), "1".getBytes(), StandardOpenOption.CREATE);

                // create namespace
                Files.createDirectories(namespacePath);

                // set path to nvme device and enable namespace
                Files.write(
                    namespacePath.resolve("device_path"),
                    nvmeVlmData.getBackingDevice().getBytes(),
                    StandardOpenOption.CREATE
                );

                Files.write(namespacePath.resolve("enable"), "1".getBytes(), StandardOpenOption.CREATE);

                // get port directory or create it if the first subsystem is being added
                LsIpAddress ipAddr = getIpAddr(nvmeRscData.getResource(), nvmeVlmData.getVlmNr().value, accCtx);
                String portIdx = getPortIdx(ipAddr, extCmd);

                if (portIdx == null)
                {
                    // get existing port directories and compute next available index
                    OutputData output = extCmd.exec(
                        "/bin/bash", "-c", "ls", "-m", "--color=never", "--directory", NVME_PORTS_PATH);
                    ExtCmdUtils.checkExitCode(
                        output,
                        StorageException::new,
                        "Failed to list files!"
                    );
                    String[] portDirs = new String(output.stdoutData).split(", ");
                    portIdx = portDirs[portDirs.length - 1] + 1;

                    Files.createDirectories(portsPath.resolve(portIdx));
                    Files.write(
                        portsPath.resolve("addr_traddr"), ipAddr.getAddress().getBytes(), StandardOpenOption.CREATE
                    );

                    // set the transport type and port
                    String transportType = nvmePrioProps.getProp(ApiConsts.KEY_TR_TYPE);
                    if (transportType == null)
                    {
                        transportType = "rdma";
                    }
                    Files.write(portsPath.resolve("addr_trtype"), transportType.getBytes(), StandardOpenOption.CREATE);

                    String port = nvmePrioProps.getProp(ApiConsts.KEY_PORT);
                    if (port == null)
                    {
                        port = Integer.toString(IANA_DEFAULT_PORT);
                    }
                    Files.write(portsPath.resolve("addr_trsvcid"), port.getBytes(), StandardOpenOption.CREATE);

                    // set the address family of the port, either IPv4 or IPv6
                    Files.write(
                        portsPath.resolve("addr_adrfam"),
                        ipAddr.getAddressType().toString().toLowerCase().getBytes(),
                        StandardOpenOption.CREATE
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
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to configure NVMe target!", exc);
        }
        catch (ValueOutOfRangeException | InvalidNameException | AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void cleanUpTarget(NvmeRscData nvmeRscData, AccessContext accCtx)
        throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();
        final String subsystemName = NVME_SUBSYSTEM_PREFIX + nvmeRscData.getResourceName();
        final String subsystemDirectory = NVME_SUBSYSTEMS_PATH + subsystemName;

        try
        {
            for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
            {
                final int namespaceNr = nvmeVlmData.getVlmNr().value + 1;
                final Path namespacePath = Paths.get(
                    subsystemDirectory + "/namespaces/" + namespaceNr
                );

                // remove soft link
                String portIdx = getPortIdx(getIpAddr(nvmeRscData.getResource(), nvmeVlmData.getVlmNr().value, accCtx), extCmd);
                if (portIdx == null)
                {
                    throw new StorageException(
                        "No ports directory for NVMe subsystem \'" + subsystemName + "\' exists!");
                }

                OutputData output = extCmd.exec(
                    "rmdir", NVME_PORTS_PATH + portIdx + "/subsystems/" + subsystemName
                );
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to remove symbolic link!");

                // delete ports directory
                output = extCmd.exec(
                    "/bin/bash", "-c",
                    "ls", "-m", "--color=never", "--directory", NVME_PORTS_PATH + portIdx + "/subsystems"
                );
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to list files!");

                if (output.stdoutData.length == 0)
                {
                    output = extCmd.exec("rmdir", NVME_PORTS_PATH + portIdx);
                    ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete ports directory!");
                }

                // disable namespace
                Files.write(namespacePath.resolve("enable"), "0".getBytes(), StandardOpenOption.CREATE);

                output = extCmd.exec("rmdir", namespacePath + Integer.toString(namespaceNr));
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete namespace directory!");

                // delete subsystem directory
                output = extCmd.exec("rmdir", subsystemDirectory);
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete subsystem directory!");
            }
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to delete NVMe target!", exc);
        }
        catch (ValueOutOfRangeException | InvalidNameException | AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void connect(NvmeRscData nvmeRscData, AccessContext accCtx)
        throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();
        final String subsystemName = NVME_SUBSYSTEM_PREFIX + nvmeRscData.getResourceName();

        try
        {
            final PriorityProps nvmePrioProps = new PriorityProps(
                nvmeRscData.getResource().getDefinition().getProps(accCtx),
                stltProps
            );

            String port = nvmePrioProps.getProp(ApiConsts.KEY_PORT);
            if (port == null)
            {
                port = Integer.toString(IANA_DEFAULT_PORT);
            }

            for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
            {
                String ipAddr = getIpAddr(
                    nvmeRscData.getResource().getDefinition().streamResource(accCtx).filter(
                        rsc -> !rsc.equals(nvmeRscData.getResource())
                    ).findFirst().orElseThrow(() -> new StorageException("Target resource not found!")),
                    nvmeVlmData.getVlmNr().value,
                    accCtx
                ).getAddress();

                List<String> subsystemNames = discover(ipAddr, port);
                if (!subsystemNames.contains(subsystemName))
                {
                    throw new StorageException("Failed to discover subsystem name \'" + subsystemName + "\'!");
                }

                OutputData output = extCmd.exec(
                    "nvme",
                    "connect",
                    "-t",
                    "rdma",
                    "-n",
                    subsystemName,
                    "-a",
                    ipAddr,
                    "-s",
                    port
                );
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to connect to NVMe target!");
            }
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to connect to NVMe target!", exc);
        }
        catch (ValueOutOfRangeException | InvalidNameException | AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void disconnect(NvmeRscData nvmeRscData) throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();
        final String subsystemName = NVME_SUBSYSTEM_PREFIX + nvmeRscData.getResourceName();

        try
        {
            for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
            {
                OutputData output = extCmd.exec("nvme", "disconnect", "-n", subsystemName);

                if (output.exitCode != 0)
                {
                    output = extCmd.exec("nvme", "disconnect", "-d", nvmeVlmData.getBackingDevice());
                }

                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to disconnect from NVMe target!");
            }
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to disconnect from NVMe target!", exc);
        }
    }

    public boolean nvmeRscExists(NvmeRscData nvmeRscData)
    {
        return Files.exists(Paths.get(NVME_SUBSYSTEMS_PATH).resolve(nvmeRscData.getResourceName().getDisplayName()));
    }

    /* helper methods */

    private List<String> discover(String ipAddr, String port)
        throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();
        List<String> subsystemNames = new ArrayList<>();

        try
        {
            OutputData output = extCmd.exec(
                "nvme",
                "discover",
                "-t",
                "rdma",
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

    private LsIpAddress getIpAddr(Resource rsc, int vlmNr, AccessContext accCtx)
        throws ValueOutOfRangeException, InvalidNameException, AccessDeniedException, InvalidKeyException
    {
        String netIfName = rsc
            .getVolume(new VolumeNumber(vlmNr))
            .getStorPool(accCtx)
            .getProps(accCtx)
            .getProp(ApiConsts.KEY_STOR_POOL_PREF_NIC);

        if (netIfName == null)
        {
            netIfName = DEFAULT_NETIF;
        }

        return rsc
            .getAssignedNode()
            .getNetInterface(accCtx, new NetInterfaceName(netIfName))
            .getAddress(accCtx);
    }

    private String getPortIdx(LsIpAddress ipAddr, ExtCmd extCmd)
        throws StorageException, IOException, ChildProcessTimeoutException
    {
        OutputData output = extCmd.exec(
            "/bin/bash", "-c", "cd " + NVME_PORTS_PATH + "; grep -r -H --color=never " + ipAddr.getAddress()
        );

        String portIdx;
        if (output.exitCode == 0)
        {
            String grepPortIdx = new String(output.stdoutData);
            portIdx = grepPortIdx.substring(0, grepPortIdx.indexOf(System.getProperty("file.separator")));
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
}
