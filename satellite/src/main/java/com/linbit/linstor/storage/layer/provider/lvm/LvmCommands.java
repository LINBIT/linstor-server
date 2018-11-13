package com.linbit.linstor.storage.layer.provider.lvm;

import static com.linbit.linstor.storage.layer.provider.utils.Commands.genericExecutor;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class LvmCommands
{
    public static final int LVS_COL_IDENTIFIER = 0;
    public static final int LVS_COL_PATH = 1;
    public static final int LVS_COL_SIZE = 2;
    public static final int LVS_COL_VG = 3;
    public static final int LVS_COL_POOL_LV = 4;

    public static OutputData lvs(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        return genericExecutor(
            extCmd,
            concat(
                new String[] {
                    "lvs",
                    "-o", "lv_name,lv_path,lv_size,vg_name,pool_lv",
                    "--separator", StorageUtils.DELIMITER,
                    "--noheadings",
                    "--units", "k",
                    "--nosuffix"
                },
                volumeGroups
            ),
            "Failed to list lvm volumes",
            "Failed to query 'lvs' info"
        );
    }

    public static OutputData getExtentSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        return genericExecutor(
            extCmd,
            concat(
                new String[] {
                    "vgs",
                    "-o", "vg_name,vg_extent_size",
                    "--separator", StorageUtils.DELIMITER,
                    "--units", "k",
                    "--noheadings",
                    "--nosuffix"
                },
                volumeGroups
            ),
            "Failed to query lvm extent size",
            "Failed to query extent size of volume group(s) " + volumeGroups
        );
    }

    public static OutputData createFat(ExtCmd extCmd, String volumeGroup, String vlmId, long size)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "lvcreate",
                "--size", size + "k",
                "-n", vlmId,
                "-y", // force, skip "wipe signature question"
                volumeGroup
            },
            "Failed to create lvm volume",
            "Failed to create new lvm volume '" + vlmId + "' in volume group '" + volumeGroup +
                "' with size " + size + "kb"
        );
    }

    public static OutputData createThin(
        ExtCmd extCmd,
        String volumeGroup,
        String thinPoolName,
        String vlmId,
        long size
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "lvcreate",
                "--virtualsize", size + "k", // -V
                "--thinpool", thinPoolName,
                "--name", vlmId,        // -n
                volumeGroup
            },
            "Failed to create lvm volume",
            "Failed to create new lvm volume '" + vlmId + "' in volume group '" + volumeGroup +
            "' with size " + size + "kb"
            );
    }

    public static OutputData delete(ExtCmd extCmd, String volumeGroup, String vlmId)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "lvremove",
                "-f", // skip the "are you sure?"
                volumeGroup + File.separator + vlmId
            },
            "Failed to delete lvm volume",
            "Failed to delete lvm volume '" + vlmId + "' from volume group '" + volumeGroup
        );
    }

    public static OutputData resize(ExtCmd extCmd, String volumeGroup, String vlmId, long size)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[]
            {
                "lvresize",
                "--size", size + "k",
                volumeGroup + File.separator + vlmId
            },
            "Failed to resize lvm volume",
            "Failed to resize lvm volume '" + vlmId + "' in volume group '" + volumeGroup + "' to size " + size
        );
    }

    public static OutputData rename(ExtCmd extCmd, String volumeGroup, String vlmCurrentId, String vlmNewId)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            new String[] {
                "lvrename",
                volumeGroup,
                vlmCurrentId,
                vlmNewId
            },
            "Failed to rename lvm volume from '" + vlmCurrentId + "' to '" + vlmNewId + "'",
            "Failed to rename lvm volume from '" + vlmCurrentId + "' to '" + vlmNewId + "'"
        );
    }

    public static OutputData getVgTotalSize(ExtCmd extCmd, Set<String> volumeGroups)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            concat(
                new String[]
                {
                    "vgs",
                   "-o", "vg_size",
                    "--units", "k",
                    "--separator", StorageUtils.DELIMITER,
                    "--noheadings",
                    "--nosuffix"
                },
                volumeGroups
            ),
            "Failed to query total size of volume group(s) " + volumeGroups,
            "Failed to query total size of volume group(s) " + volumeGroups
        );
    }

    public static OutputData getVgFreeSize(ExtCmd extCmd, Set<String> volumeGroups)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            concat(
                new String[]
                {
                    "vgs",
                    "-o", "vg_name,vg_free",
                    "--units", "k",
                    "--separator", StorageUtils.DELIMITER,
                    "--noheadings",
                    "--nosuffix"
                },
                volumeGroups
            ),
            "Failed to query free size of volume group(s) " + volumeGroups,
            "Failed to query free size of volume group(s) " + volumeGroups
        );
    }

    public static OutputData getVgThinTotalSize(ExtCmd extCmd, Set<String> volumeGroups)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            concat(
                new String[]
                    {
                        "lvs",
                        "-o", "lv_size",
                        "--units", "k",
                        "--separator", StorageUtils.DELIMITER,
                        "--noheadings",
                        "--nosuffix"
                    },
                    volumeGroups
                ),
            "Failed to query total size of volume group(s) " + volumeGroups,
            "Failed to query total size of volume group(s) " + volumeGroups
            );
    }

    public static OutputData getVgThinFreeSize(ExtCmd extCmd, Set<String> volumeGroups)
        throws StorageException
    {
        return genericExecutor(
            extCmd,
            concat(
                new String[]
                    {
                        "vgs",
                        "-o", "lv_name,lv_size,data_percent",
                        "--units", "k",
                        "--separator", StorageUtils.DELIMITER,
                        "--noheadings",
                        "--nosuffix"
                    },
                    volumeGroups
                ),
            "Failed to query free size of volume group(s) " + volumeGroups,
            "Failed to query free size of volume group(s) " + volumeGroups
            );
    }

    private static String[] concat(String[] array, Set<String> list)
    {
        List<String> result = new ArrayList<>(Arrays.asList(array));
        result.addAll(list);
        return result.toArray(new String[0]);
    }
}
