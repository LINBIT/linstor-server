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
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;

import static com.linbit.linstor.api.ApiConsts.DEFAULT_NETIF;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class NvmeUtils
{
    public static final String NVME_SUBSYSTEM_PREFIX = "LS-NVMe_";
    private static final String NVMET_PATH = "/sys/kernel/config/nvmet";
    private static final int IANA_DEFAULT_PORT = 4420;

    private final ExtCmdFactory extCmdFactory;

    public NvmeUtils(ExtCmdFactory extCmdFactoryRef)
    {
        extCmdFactory = extCmdFactoryRef;
    }

    public void configureTarget(NvmeRscData nvmeRscData, AccessContext accCtx)
        throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();
        final String portsDirectory = NVMET_PATH + "/ports/";

        int vlmCount = nvmeRscData.getResource().getVolumeCount();
        for (int vlmNr = 0; vlmNr < vlmCount; vlmNr++)
        {
            final String subsystemName = NVME_SUBSYSTEM_PREFIX + nvmeRscData.getResourceName() + "_" + vlmNr;
            final String subsystemDirectory = NVMET_PATH + "/subsystems/" + subsystemName;

            try
            {
                // create nvmet-rdma subsystem
                Path path = Paths.get(subsystemDirectory);
                Files.createDirectory(path);

                // allow any host to be connected
                path = Paths.get(subsystemDirectory + "/attr_allow_any_host");
                Files.write(path, "1".getBytes());

                // create namespace and cd to it
                path = Paths.get(subsystemDirectory + "/namespaces/" + 10); //FIXME dummy
                Files.createDirectory(path);

                // set path to nvme device and enable namespace
                path = Paths.get(subsystemDirectory + "/namespaces/" + 10 + "/device_path"); //FIXME dummy
                Files.write(path, ("/dev/" + "nvme2n1").getBytes()); //FIXME dummy

                path = Paths.get(subsystemDirectory + "/namespaces/" + 10 + "/enable"); //FIXME dummy
                Files.write(path, "1".getBytes());

                // get port directory or create it if the first subsystem is being added
                LsIpAddress ipAddr = getIpAddr(nvmeRscData, vlmNr, accCtx);
                String portIdx = getPortIdx(ipAddr, extCmd);

                if (portIdx == null)
                {
                    // get existing port directories and compute next available index
                    OutputData output = extCmd.exec(
                        "/bin/bash", "-c", "ls", "-m", "--color=never", "--directory");
                    ExtCmdUtils.checkExitCode(
                        output,
                        StorageException::new,
                        "Failed to list files!"
                    );
                    String[] portDirs = new String(output.stdoutData).split(", ");
                    portIdx = portDirs[portDirs.length - 1] + 1;

                    path = Paths.get(portsDirectory + portIdx);
                    Files.createDirectory(path);

                    path = Paths.get(portsDirectory + "/addr_traddr");
                    Files.write(path, ipAddr.getAddress().getBytes());

                    // set RDMA as transport type and set RDMA port
                    path = Paths.get(portsDirectory + "/addr_trtype");
                    Files.write(path, "rdma".getBytes());
                    path = Paths.get(portsDirectory + "/addr_trsvcid");
                    Files.write(path, (new Integer(IANA_DEFAULT_PORT)).toString().getBytes());

                    // set the address family of the port, either IPv4 or IPv6
                    path = Paths.get(portsDirectory + "/addr_adrfam");
                    Files.write(path, ipAddr.getAddressType().toString().getBytes());

                }

                // create soft link
                ExtCmdUtils.checkExitCode(
                    extCmd.exec("ln", "-s", subsystemDirectory, portsDirectory + portIdx + "/subsystems/" + subsystemName),
                    StorageException::new,
                    "Failed to create symbolic link!"
                );
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
    }

    public void cleanUpTarget(NvmeRscData nvmeRscData, AccessContext accCtx)
        throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();
        final String portsDirectory = NVMET_PATH + "/ports/";

        int vlmCount = nvmeRscData.getResource().getVolumeCount();
        for (int vlmNr = 0; vlmNr < vlmCount; vlmNr++)
        {
            final String subsystemName = NVME_SUBSYSTEM_PREFIX + nvmeRscData.getResourceName() + "_" + vlmNr;
            final String subsystemDirectory = NVMET_PATH + "/subsystems/" + subsystemName;

            try
            {
                // remove soft link
                OutputData output = extCmd.exec("cd", portsDirectory);
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to change current directory!");

                String portIdx = getPortIdx(getIpAddr(nvmeRscData, vlmNr, accCtx), extCmd);
                if (portIdx == null)
                {
                    throw new StorageException(
                        "No ports directory for NVMe subsystem \'" + subsystemName + "\' exists!");
                }

                output = extCmd.exec("rm", portsDirectory + portIdx + "/subsystems/" + subsystemName);
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to remove symbolic link!");

                // delete ports directory
                output = extCmd.exec(
                    "/bin/bash", "-c",
                    "ls", "-m", "--color=never", "--directory", portIdx + "/subsystems"
                );
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to list files!");

                if (output.stdoutData.length == 0)
                {
                    output = extCmd.exec("rmdir", portIdx);
                    ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete ports directory!");
                }

                // disable namespace
                output = extCmd.exec("cd", subsystemDirectory + "/namespaces/" + 10); // FIXME: dummy
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to change current directory!");

                Path path = Paths.get(subsystemDirectory + "/namespaces/" + 10 + "/enable"); //FIXME dummy
                Files.write(path, "0".getBytes());

                // delete namespace directory
                output = extCmd.exec("cd", "..");
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to change current directory!");

                output = extCmd.exec("rmdir", "10"); // FIXME: dummy
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete namespace directory!");

                // delete subsystem directory
                output = extCmd.exec("cd", "../..");
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to change current directory!");

                output = extCmd.exec("rmdir", subsystemName);
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to delete subsystem directory!");
            }
            catch (
                IOException | ChildProcessTimeoutException |
                ValueOutOfRangeException | InvalidNameException |
                AccessDeniedException |
                InvalidKeyException exc
            ) // TODO: right?
            {
                throw new StorageException("Failed to delete NVMe target!", exc);
            }
        }
    }

    private List<String> discover(String ipAddr)
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
                (new Integer(IANA_DEFAULT_PORT)).toString()
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

        if (subsystemNames.isEmpty())
        {
            throw new StorageException("No NVMe subsystems found!");
        }

        return subsystemNames;
    }

    public void connect(String ipAddr)
        throws StorageException
    {
        ExtCmd extCmd = extCmdFactory.create();

        try
        {
            List<String> subsystemNames = discover(ipAddr);
            for (String subsystemName : subsystemNames)
            {
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
                    (new Integer(IANA_DEFAULT_PORT)).toString());
                ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to create symbolic link!");
            }
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to connect to NVMe target!", exc);
        }
    }

    public void disconnect(String subsystemName, int nameSpaceNr) throws StorageException
    {
        final ExtCmd extCmd = extCmdFactory.create();

        try
        {
            OutputData output = extCmd.exec("nvme", "disconnect", "-n", subsystemName);

            if (output.exitCode != 0)
            {
                Path path = Paths.get(
                    NVMET_PATH + "/subsystems/" + subsystemName + "/namespaces/" + nameSpaceNr + "/device_path"
                );
                if (Files.notExists(path))
                {
                    throw new StorageException("No device path configured for NVMe target!");
                }

                output = extCmd.exec("nvme", "disconnect", "-d", Files.newBufferedReader(path).readLine());
            }

            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to disconnect from NVMe target!");
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to disconnect from NVMe target!", exc);
        }
    }

    private LsIpAddress getIpAddr(NvmeRscData nvmeRscData, int vlmNr, AccessContext accCtx)
        throws ValueOutOfRangeException, InvalidNameException, AccessDeniedException, InvalidKeyException
    {
        String netIfName = nvmeRscData
            .getResource()
            .getVolume(new VolumeNumber(vlmNr))
            .getStorPool(accCtx)
            .getProps(accCtx)
            .getProp(ApiConsts.KEY_STOR_POOL_PREF_NIC);

        if (netIfName == null)
        {
            netIfName = DEFAULT_NETIF;
        }

        return nvmeRscData
            .getResource()
            .getAssignedNode()
            .getNetInterface(accCtx, new NetInterfaceName(netIfName))
            .getAddress(accCtx);
    }

    private String getPortIdx(LsIpAddress ipAddr, ExtCmd extCmd)
        throws StorageException, IOException, ChildProcessTimeoutException
    {
        OutputData output = extCmd.exec("/bin/bash", "-c", "grep -H --color=never " + ipAddr);

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
