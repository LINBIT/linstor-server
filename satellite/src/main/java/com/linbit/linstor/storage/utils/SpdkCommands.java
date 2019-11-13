package com.linbit.linstor.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.provider.utils.Commands;
import com.linbit.linstor.storage.layer.provider.utils.Commands.RetryHandler;
import com.linbit.linstor.storage.layer.provider.utils.RetryIfDeviceBusy;
import com.linbit.utils.StringUtils;

import java.io.File;

import static com.linbit.linstor.storage.layer.provider.utils.Commands.genericExecutor;

public class SpdkCommands
{
     // requires "/usr/bin/rpc.py" symlink to "spdk-19.07/scripts/rpc.py" script in host OS
    public static final String SPDK_RPC_SCRIPT = "rpc.py";

    public static OutputData lvs(ExtCmd extCmd) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "get_bdevs"
            },
            "Failed to list bdevs",
            "Failed to query 'get_bdevs' info"
        );
    }

    public static OutputData lvsByName(ExtCmd extCmd, String name) throws StorageException
    {
        return genericExecutor(
                extCmd,
                new String[]
                {
                    SPDK_RPC_SCRIPT,
                    "get_bdevs",
                    "--name",
                    name
                },
                "Failed to list bdevs",
                "Failed to query 'get_bdevs' info"
        );
    }

    public static OutputData getLvolStores(ExtCmd extCmd) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "get_lvol_stores"
            },
            "Failed to query lvol stores extent size",
            "Failed to query extent size of volume group(s)"
        );
    }

    public static OutputData createFat(
        ExtCmd extCmd,
        String volumeGroup,
        String vlmId,
        long size,
        String... additionalParameters
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "construct_lvol_bdev",
                vlmId,
                String.valueOf(size/1024), // KiB
                "--lvs-name", volumeGroup
            },
            "Failed to create lvol bdev",
            "Failed to create new lvol bdev'" + vlmId + "' in lovl store '" + volumeGroup +
            "' with size " + size + "mb"
        );
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
                    "construct_lvol_bdev",
                    vlmId,
                    String.valueOf(size/1024), // KiB
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

    public static OutputData delete(ExtCmd extCmd, String volumeGroup, String vlmId)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "destroy_lvol_bdev",
                volumeGroup + File.separator + vlmId
            },
            "Failed to delete lvol bdev",
            "Failed to delete lvm volume '" + vlmId + "' from volume group '" + volumeGroup,
            new RetryIfDeviceBusy()
        );
    }

    public static OutputData resize(ExtCmd extCmd, String volumeGroup, String vlmId, long size)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "resize_lvol_bdev",
                volumeGroup + File.separator + vlmId,
                String.valueOf(size/1024), // KiB
            },
            "Failed to resize lvol bdev",
            "Failed to resize lvol bdev '" + vlmId + "' in lvol store '" + volumeGroup + "' to size " + size
        );
    }

    public static OutputData rename(ExtCmd extCmd, String volumeGroup, String vlmCurrentId, String vlmNewId)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "rename_lvol_bdev",
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

    public static OutputData createTransport(ExtCmd extCmd, String type)
            throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                SPDK_RPC_SCRIPT,
                "nvmf_create_transport",
                "--trtype",
                type
            },
            "Failed to create transport'" + type,
            "Failed to create transport' '" + type + "'",
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

    public static OutputData getNvmfSubsystems(ExtCmd extCmd) throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                    SPDK_RPC_SCRIPT,
                    "get_nvmf_subsystems"
            },
            "Failed to query nvmf subsystems",
            "Failed to query nvmf subsystems"
        );
    }
}
