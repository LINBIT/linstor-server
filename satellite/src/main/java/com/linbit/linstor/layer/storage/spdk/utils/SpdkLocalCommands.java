package com.linbit.linstor.layer.storage.spdk.utils;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.layer.storage.spdk.SpdkCommands;
import com.linbit.linstor.layer.storage.utils.RetryIfDeviceBusy;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.linstor.storage.utils.Commands.RetryHandler;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.storage.utils.Commands.genericExecutor;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Singleton
public class SpdkLocalCommands implements SpdkCommands<OutputData>
{
    // requires "/usr/bin/rpc.py" symlink to "spdk/scripts/rpc.py" script in host OS
    public static final String SPDK_RPC_SCRIPT = "rpc.py";

    private final ExtCmdFactory extCmdFactory;

    private final ObjectMapper objectMapper;

    @Inject
    public SpdkLocalCommands(ExtCmdFactory extCmdFactoryRef)
    {
        extCmdFactory = extCmdFactoryRef;
        objectMapper = new ObjectMapper();
    }

    @Override
    public Iterator<JsonNode> getJsonElements(OutputData output)
        throws StorageException
    {
        JsonNode rootNode = null;

        try
        {
            rootNode = objectMapper.readTree(output.stdoutData);
        }
        catch (IOException ioExc)
        {
            throw new StorageException("I/O error while parsing SPDK response");
        }

        return rootNode.elements();
    }

    @Override
    public OutputData lvs() throws StorageException
    {
        return lvs(extCmdFactory.create());
    }

    public static OutputData lvs(ExtCmd extCmd) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_get_bdevs" // get_bdevs is deprecated
            },
            "Failed to list bdevs",
            "Failed to query 'get_bdevs' info"
        );
    }

    @Override
    public OutputData lvsByName(String name) throws StorageException
    {
        return lvsByName(extCmdFactory.create(), name);
    }

    public static OutputData lvsByName(ExtCmd extCmd, String name) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_get_bdevs", // get_bdevs is deprecated
                "--name", name
            },
            "Failed to list bdevs",
            "Failed to query 'get_bdevs' info"
        );
    }

    @Override
    public OutputData getLvolStores() throws StorageException
    {
        return getLvolStores(extCmdFactory.create());
    }

    public static OutputData getLvolStores(ExtCmd extCmd) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_lvol_get_lvstores" // get_lvol_stores is deprecated
            },
            "Failed to query lvol stores extent size",
            "Failed to query extent size of volume group(s)"
        );
    }

    @Override
    public OutputData createFat(
        String volumeGroup,
        String vlmId,
        long sizeInKib,
        String... additionalParameters
    )
        throws StorageException
    {
        return createFat(extCmdFactory.create(), volumeGroup, vlmId, sizeInKib, additionalParameters);
    }

    public static OutputData createFat(
        ExtCmd extCmd,
        String volumeGroup,
        String vlmId,
        long sizeInKib,
        String... additionalParameters
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_lvol_create", // construct_lvol_bdev is deprecated
                vlmId,
                String.valueOf(
                    SizeConv.convertRoundUp(sizeInKib, SizeUnit.UNIT_KiB, SizeUnit.UNIT_MiB)
                ),
                "--lvs-name", volumeGroup
            },
            "Failed to create lvol bdev",
            "Failed to create new lvol bdev'" + vlmId + "' in lovl store '" + volumeGroup +
                "' with size " + sizeInKib + "mb"
        );
    }

    public OutputData createThin(
        String volumeGroup,
        String thinPoolName,
        String vlmId,
        long size,
        String... additionalParameters
    )
        throws StorageException
    {
        return createThin(extCmdFactory.create(), volumeGroup, thinPoolName, vlmId, size, additionalParameters);
    }

    public static OutputData createThin(
        ExtCmd extCmd,
        String volumeGroup,
        String thinPoolName,
        String vlmId,
        long size,
        String... additionalParameters
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            StringUtils.concat(
                new String[]
                {
                    SPDK_RPC_SCRIPT,
                    "bdev_lvol_create", // construct_lvol_bdev is deprecated
                    vlmId,
                    String.valueOf(
                        SizeConv.convert(size, SizeUnit.UNIT_KiB, SizeUnit.UNIT_MiB)
                    ),
                    "--lvs-name", volumeGroup,
                    "--thin-provision"
                },
                additionalParameters
            ),
            "Failed to create lvol bdev",
            "Failed to create new lvol bdev'" + vlmId + "' in lovl store '" + volumeGroup +
                "' with size " + size + "mb"
        );
    }

    @Override
    public OutputData createSnapshot(
        String fullQualifiedVlmIdRef,
        String snapName
    )
        throws StorageException
    {
        return createSnapshot(extCmdFactory.create(), fullQualifiedVlmIdRef, snapName);
    }

    public static OutputData createSnapshot(
        ExtCmd extCmd,
        String fullQualifiedVlmId,
        String snapName
    )
        throws StorageException
    {
        String errMsg = "Failed to create snapshot '" + snapName + "' of vol '" + fullQualifiedVlmId + "'";
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_lvol_snapshot",
                fullQualifiedVlmId,
                snapName
            },
            errMsg,
            errMsg
        );
    }

    @Override
    public OutputData restoreSnapshot(String fullQualifiedSnapName, String newVlmId)
        throws StorageException, AccessDeniedException
    {
        return restoreSnapshot(extCmdFactory.create(), fullQualifiedSnapName, newVlmId);
    }

    public static OutputData restoreSnapshot(
        ExtCmd extCmd,
        String fullQualifiedSnapId,
        String newVlmId
    )
        throws StorageException
    {
        String errMsg = "Failed to restore snapshot '" + fullQualifiedSnapId + "' into new volume '" + newVlmId + "'";
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_lvol_clone",
                fullQualifiedSnapId,
                newVlmId
            },
            errMsg,
            errMsg
        );
    }

    @Override
    public OutputData decoupleParent(String fullQualifiedIdentifierRef) throws StorageException, AccessDeniedException
    {
        return decoupleParent(extCmdFactory.create(), fullQualifiedIdentifierRef);
    }

    public static OutputData decoupleParent(
        ExtCmd extCmd,
        String fullQualifiedVlmId
    )
        throws StorageException
    {
        String errMsg = "Failed to 'decouple_parent' for volume '" + fullQualifiedVlmId + "'";
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_lvol_decouple_parent",
                fullQualifiedVlmId
            },
            errMsg,
            errMsg
        );
    }

    @Override
    public OutputData clone(String fullQualSnapNameRef, String lvTargetIdRef)
        throws StorageException, AccessDeniedException
    {
        return clone(extCmdFactory.create(), fullQualSnapNameRef, lvTargetIdRef);
    }

    public static OutputData clone(
        ExtCmd extCmd,
        String fullQualifiedSourceSnapId,
        String lvTargetIdRef
    )
        throws StorageException
    {
        String errMsg = "Failed to 'bdev_lvol_clone' from source snapshot '" + fullQualifiedSourceSnapId +
            "' to new volume '" + lvTargetIdRef + "'";
        return genericExecutor(
            extCmd,
            new String[]
                {
                    SPDK_RPC_SCRIPT,
                    "bdev_lvol_clone",
                    fullQualifiedSourceSnapId,
                lvTargetIdRef
                },
                errMsg,
                errMsg
            );
    }

    @Override
    public OutputData delete(String volumeGroup, String vlmId)
        throws StorageException
    {
        return delete(extCmdFactory.create(), volumeGroup, vlmId);
    }

    public static OutputData delete(ExtCmd extCmd, String volumeGroup, String vlmId)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_lvol_delete", // destroy_lvol_bdev is deprecated
                volumeGroup + File.separator + vlmId
            },
            "Failed to delete lvol bdev",
            "Failed to delete lvm volume '" + vlmId + "' from volume group '" + volumeGroup,
            new RetryIfDeviceBusy()
        );
    }

    @Override
    public OutputData resize(String volumeGroup, String vlmId, long sizeInKib)
        throws StorageException
    {
        return resize(extCmdFactory.create(), volumeGroup, vlmId, sizeInKib);
    }

    public static OutputData resize(ExtCmd extCmd, String volumeGroup, String vlmId, long sizeInKib)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_lvol_resize", // resize_lvol_bdev is deprecated
                volumeGroup + File.separator + vlmId,
                String.valueOf(
                    SizeConv.convert(sizeInKib, SizeUnit.UNIT_KiB, SizeUnit.UNIT_MiB)
                ),
            },
            "Failed to resize lvol bdev",
            "Failed to resize lvol bdev '" + vlmId + "' in lvol store '" + volumeGroup + "' to size " + sizeInKib
        );
    }

    @Override
    public OutputData rename(String volumeGroup, String vlmCurrentId, String vlmNewId)
        throws StorageException
    {
        return rename(extCmdFactory.create(), volumeGroup, vlmCurrentId, vlmNewId);
    }

    public static OutputData rename(ExtCmd extCmd, String volumeGroup, String vlmCurrentId, String vlmNewId)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_lvol_rename", // rename_lvol_bdev is deprecated
                volumeGroup + File.separator + vlmCurrentId,
                vlmNewId
            },
            "Failed to rename lvm volume from '" + vlmCurrentId + "' to '" + vlmNewId + "'",
            "Failed to rename lvm volume from '" + vlmCurrentId + "' to '" + vlmNewId + "'",
            new RetryHandler()
            {
                @Override
                public boolean retry(OutputData outputData)
                {
                    return false;
                }

                @Override
                public boolean skip(OutputData outData)
                {
                    boolean skip = false;

                    byte[] stdoutData = outData.stdoutData;
                    String output = new String(stdoutData);

                    if (output.contains("Lvol store group \"" + volumeGroup + "\" not found"))
                    {
                        // well - resource is gone... with the whole volume-group
                        skip = true;
                    }
                    return skip;
                }
            }
        );
    }

    @Override
    public void ensureTransportExists(String type)
        throws StorageException
    {
        Iterator<JsonNode> jsonElements = getJsonElements(getNvmfTransport(extCmdFactory.create()));
        if (!SpdkRemoteCommands.typeExists(type, jsonElements))
        {
            createTransport(extCmdFactory.create(), type);
        }
    }

    private OutputData getNvmfTransport(ExtCmd extCmd) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT, "nvmf_get_transports"
            },
            "Failed to get nvmf transports",
            "Failed to get nvmf transports"
        );
    }

    public static OutputData createTransport(ExtCmd extCmd, String type)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "nvmf_create_transport",
                "--trtype", type
            },
            "Failed to create transport '" + type + "'",
            "Failed to create transport '" + type + "'",
            new Commands.SkipExitCodeRetryHandler()
            {
                @Override
                public boolean retry(OutputData outputData)
                {
                    return false;
                }

                @Override
                public boolean skip(OutputData outData)
                {
                    boolean skip = false;

                    byte[] stdoutData = outData.stdoutData;
                    String output = new String(stdoutData);

                    if (output.contains("already exists"))
                    {
                        // transport type RDMA is already present
                        skip = true;
                    }
                    return skip;
                }
            }
        );
    }

    @Override
    public OutputData getNvmfSubsystems() throws StorageException
    {
        return getNvmfSubsystems(extCmdFactory.create());
    }

    public static OutputData getNvmfSubsystems(ExtCmd extCmd) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "nvmf_get_subsystems" // get_nvmf_subsystems is deprecated
            },
            "Failed to query nvmf subsystems",
            "Failed to query nvmf subsystems"
        );
    }

    @Override
    public OutputData nvmSubsystemCreate(String subsystemName) throws StorageException, AccessDeniedException
    {
        return genericExecutor(
            extCmdFactory.create(),
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "nvmf_subsystem_create",
                subsystemName,
                "--allow-any-host"
            },
            "Failed to create subsystem!",
            "Failed to create subsystem!"
        );
    }

    @Override
    public OutputData nvmfSubsystemAddListener(
        String subsystemName,
        String transportType,
        String address,
        String addressType,
        String port
    )
        throws StorageException, AccessDeniedException
    {
        return genericExecutor(
            extCmdFactory.create(),
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "nvmf_subsystem_add_listener",
                subsystemName,
                "-t", transportType,
                "-a", address,
                "-f", addressType,
                "-s", port
            },
            "Failed to add listener to subsystem!",
            "Failed to add listener to subsystem!"
        );
    }

    @Override
    public OutputData nvmfSubsystemAddNs(String subsystemNameRef, String spdkPath)
        throws StorageException, AccessDeniedException
    {
        return genericExecutor(
            extCmdFactory.create(),
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "nvmf_subsystem_add_ns",
                subsystemNameRef,
                spdkPath
            },
            "Failed to create namespace!",
            "Failed to create namespace!"
        );
    }

    @Override
    public OutputData nvmfDeleteSubsystem(String subsystemName) throws StorageException
    {
        return genericExecutor(
            extCmdFactory.create(),
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "delete_nvmf_subsystem",
                subsystemName
            },
            "Failed to delete subsystem!",
            "Failed to delete subsystem!"
        );
    }

    @Override
    public OutputData nvmfSubsystemRemoveNamespace(String subsystemName, int namespaceNr)
        throws StorageException, AccessDeniedException
    {
        return genericExecutor(
            extCmdFactory.create(),
            new String[] {
                SPDK_RPC_SCRIPT,
                "nvmf_subsystem_remove_ns",
                subsystemName,
                String.valueOf(namespaceNr)
            },
            "Failed to delete namespace!",
            "Failed to delete namespace!"
        );
    }


    public static OutputData nvmeBdevCreate(ExtCmd extCmd, String pciAddress)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_nvme_attach_controller", // construct_nvme_bdev is deprecated
                "--trtype", "PCIe",
                "--traddr", pciAddress,
                "--name", pciAddress
            },
            "Failed to create nvme bdev",
            "Failed to create new nvme bdev with PCI address '" + pciAddress + "'"
        );
    }

    public static OutputData nvmeRaidBdevCreate(
        ExtCmd extCmd,
        String raidBdevName,
        List<String> baseBdevs
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_raid_create", // construct_raid_bdev is deprecated
                "--name", raidBdevName,
                "--raid-level", "0", // SPDK v19.07 supports only RAID 0
                "--strip-size_kb", "64",
                "--base-bdevs", String.join(" ", baseBdevs)
            },
            "Failed to create RAID nvme bdev",
            "Failed to create new RAID nvme bdev '" + raidBdevName + "' from bdevs: " +
                String.join(", ", baseBdevs)
        );
    }

    public static OutputData lvolStoreCreate(
        ExtCmd extCmd,
        String bdevName,
        String lvolStoreName
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_lvol_create_lvstore", // construct_lvol_store is deprecated
                bdevName,
                lvolStoreName
            },
            "Failed to create lvol store",
            "Failed to create new lvol store '" + lvolStoreName + "' on bdev '" + bdevName + "'"
        );
    }

    public static OutputData nvmeBdevRemove(ExtCmd extCmd, String controllerName)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_nvme_detach_controller", // delete_nvme_controller is deprecated
                controllerName
            },
            "Failed to remove nvme bdev",
            "Failed to remove nvme bdev '" + controllerName + "'"
        );
    }

    public static OutputData nvmeRaidBdevRemove(ExtCmd extCmd, String raidBdevName)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_raid_delete", // destroy_raid_bdev is deprecated
                raidBdevName
            },
            "Failed to remove RAID nvme bdev",
            "Failed to remove RAID nvme bdev '" + raidBdevName + "'"
        );
    }

    public static OutputData lvolStoreRemove(ExtCmd extCmd, String lvolStoreName)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_lvol_delete_lvstore", // destroy_lvol_store is deprecated
                "-l", lvolStoreName
            },
            "Failed to remove lvol store",
            "Failed to remove lvol store '" + lvolStoreName + "'"
        );
    }

    public static OutputData listRaidBdevsAll(ExtCmd extCmd)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "bdev_raid_get_bdevs", // get_raid_bdevs is deprecated
                "all"
            },
            "Failed to read RAID bdevs",
            "Failed to read RAID bdevs"
        );
    }
}
