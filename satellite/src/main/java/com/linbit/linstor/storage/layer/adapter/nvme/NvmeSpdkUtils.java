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
import com.linbit.linstor.core.objects.*;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.utils.SpdkCommands;
import com.linbit.linstor.storage.utils.SpdkUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;

import static com.linbit.linstor.api.ApiConsts.KEY_PREF_NIC;
import static com.linbit.linstor.storage.layer.adapter.nvme.NvmeUtils.STANDARD_NVME_SUBSYSTEM_PREFIX;
import static com.linbit.linstor.storage.utils.SpdkCommands.SPDK_RPC_SCRIPT;
import static com.linbit.linstor.storage.utils.SpdkUtils.SPDK_PATH_PREFIX;


@Singleton
public class NvmeSpdkUtils
{
    private static final int IANA_DEFAULT_PORT = 4420;

    private final ExtCmdFactory extCmdFactory;
    private final Props stltProps;
    private final ErrorReporter errorReporter;

    @Inject
    public NvmeSpdkUtils(
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
        errorReporter.logDebug(
                "NVMe: creating subsystem on target: " + STANDARD_NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
        );

        SpdkCommands.createTransport(extCmdFactory.create(),"RDMA");

        final String subsystemName = STANDARD_NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName();

        try {
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
                createSpdkNamespace(nvmeVlmData, subsystemName); //@ originally ther
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
        final String subsystemName = STANDARD_NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName();

        try
        {
            errorReporter.logDebug(
                "NVMe: cleaning up target: " + STANDARD_NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
            );

            OutputData output = extCmdFactory.create().exec(
                    SPDK_RPC_SCRIPT,
                    "delete_nvmf_subsystem",
                    subsystemName
            );
            ExtCmdUtils.checkExitCode(output, StorageException::new, "Failed to create subsystem!");

            nvmeRscData.setExists(false);
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            throw new StorageException("Failed to delete NVMe target!", exc);
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
            "NVMe: checking if subsystem " + STANDARD_NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName() + " exists."
        );

        boolean exists = false;
        try {
            exists = SpdkUtils.checkTargetExists(extCmdFactory.create(),STANDARD_NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName());

        } catch (StorageException e) {
            e.printStackTrace();
        }
        return exists;
    }

    /**
     * Creates a new directory for the given file path, sets the appropriate backing device and enables the namespace
     *
     * @param nvmeVlmData nvmeVlm object containing information for path to the new namespace
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
     * Disables the namespace at the given file path and deletes the directory
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

    /* helper methods */

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

    public ExtCmdFactory getExtCmdFactory()
    {
        return extCmdFactory;
    }
}
