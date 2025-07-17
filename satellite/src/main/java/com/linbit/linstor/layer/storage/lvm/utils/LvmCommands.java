package com.linbit.linstor.layer.storage.lvm.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.layer.storage.utils.RetryIfDeviceBusy;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.RaidLevel;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.linstor.storage.utils.Commands.RetryHandler;

import static com.linbit.linstor.storage.utils.Commands.genericExecutor;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class LvmCommands
{
    /*
     * DO NOT add "'a|.*|'" at the end of the filter list. If a block device has multiple paths (symlinks) the device is
     * accepted if ANY of the paths matches. Only if all paths either have a reject first of are undecided, the device
     * is rejected.
     *
     * We had cases where we rejected "/dev/drbd" but LVM still found (the suspended) DRBD device via
     * /dev/block/147:1000. The reason LVM took that path is because we allowed "everything else" (using "'a|.*|'")
     */
    private static final String LVM_CONF_IGNORE_DRBD_DEVICES = "devices { filter=[\"r|^/dev/drbd.*|\"] }";

    public static final int LVS_COL_IDENTIFIER = 0;
    public static final int LVS_COL_PATH = 1;
    public static final int LVS_COL_SIZE = 2;
    public static final int LVS_COL_VG = 3;
    public static final int LVS_COL_POOL_LV = 4;
    public static final int LVS_COL_DATA_PERCENT = 5;
    public static final int LVS_COL_ATTRIBUTES = 6;
    public static final int LVS_COL_METADATA_PERCENT = 7;
    public static final int LVS_COL_CHUNK_SIZE = 8;
    public static final int LVS_COL_STRIPES = 9;

    public static final int LVS_COLUMN_COUNT = 10;

    public static final int VGS_COL_VG_NAME = 0;
    public static final int VGS_COL_VG_EXTENT_SIZE = 1;
    public static final int VGS_COL_VG_SIZE = 2;
    public static final int VGS_COL_VG_FREE = 3;
    public static final int VGS_COL_LV_NAME = 4; // only available in vgsThin
    public static final int VGS_COL_LV_SIZE = 5; // only available in vgsThin
    public static final int VGS_COL_DATA_PERCENT = 6; // only available in vgsThin

    public static final int VGS_THICK_COLUMN_COUNT = 4;
    public static final int VGS_THIN_COLUMN_COUNT = 7;

    public static final String LVM_TAG_CLONE_SNAPSHOT = "linstor_clone_snapshot";


    private static String[] buildCmd(
        String baseCmd,
        @Nullable String lvmConfig,
        String[] appendedString,
        String... baseOptions
    )
    {
        return buildCmd(baseCmd, lvmConfig, Arrays.asList(appendedString), baseOptions);
    }

    private static String[] buildCmd(
        String baseCmd,
        @Nullable String lvmConfig,
        @Nullable Collection<String> appendedString,
        String... baseOptions
    )
    {
        ArrayList<String> list = new ArrayList<>();
        list.add(baseCmd);
        if (lvmConfig != null && !lvmConfig.isEmpty())
        {
            list.add("--config");
            list.add(lvmConfig);
        }
        Collections.addAll(list, baseOptions);
        if (appendedString != null)
        {
            list.addAll(appendedString);
        }
        String[] cmdArr = new String[list.size()];
        list.toArray(cmdArr);
        return cmdArr;
    }

    public static synchronized OutputData lvs(ExtCmd extCmd, Set<String> volumeGroups, String lvmConfig)
        throws StorageException
    {
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(true),
            buildCmd(
                "lvs",
                lvmConfig,
                volumeGroups,
                "-o", "lv_name,lv_path,lv_size,vg_name,pool_lv,data_percent,lv_attr,metadata_percent,chunk_size," +
                    "stripes",
                "--separator", LvmUtils.DELIMITER,
                "--noheadings",
                "--units", "k",
                "--nosuffix"
            ),
            "Failed to list lvm volumes",
            "Failed to query 'lvs' info",
            Commands.SKIP_EXIT_CODE_CHECK
        );
    }

    public static synchronized OutputData vgsThick(ExtCmd extCmd, @Nullable Set<String> volumeGroups, String lvmConfig)
        throws StorageException
    {
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(true),
            buildCmd(
                "vgs",
                lvmConfig,
                volumeGroups,
                "-o", "vg_name,vg_extent_size,vg_size,vg_free",
                "--separator", LvmUtils.DELIMITER,
                "--units", "k",
                "--noheadings",
                "--nosuffix"
            ),
            "Failed to query vgs",
            "Failed to query vgs of volume group(s) " + volumeGroups,
            Commands.SKIP_EXIT_CODE_CHECK
        );
    }

    public static synchronized OutputData vgsThin(ExtCmd extCmd, @Nullable Set<String> volumeGroups, String lvmConfig)
        throws StorageException
    {
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(true),
            buildCmd(
                "vgs",
                lvmConfig,
                volumeGroups,
                "-o", "vg_name,vg_extent_size,vg_size,vg_free,lv_name,lv_size,data_percent",
                "--separator", LvmUtils.DELIMITER,
                "--units", "k",
                "--noheadings",
                "--nosuffix"
                ),
            "Failed to query vgs",
            "Failed to query vgs of volume group(s) " + volumeGroups,
            Commands.SKIP_EXIT_CODE_CHECK
        );
    }

    public static synchronized OutputData createFat(
        ExtCmd extCmd,
        String volumeGroup,
        String vlmId,
        long size,
        @Nullable String lvmConfig,
        String... additionalParameters
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvcreate",
                lvmConfig,
                additionalParameters,
                "--size", size + "k",
                volumeGroup,
                "-n", vlmId,
                "-y" // force, skip "wipe signature question"
            ),
            "Failed to create lvm volume",
            "Failed to create new lvm volume '" + vlmId + "' in volume group '" + volumeGroup +
                "' with size " + size + "kb"
        );
    }

    public static synchronized OutputData createThinPool(
        ExtCmd extCmd,
        String volumeGroupFull,
        String thinPoolName,
        String lvmConfig,
        String... additionalParameters
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvcreate",
                lvmConfig,
                additionalParameters,
                "-l", "100%FREE",
                "-T",
                "-n", volumeGroupFull + "/" +
                    thinPoolName
            ),
            "Failed to create lvm volume",
            "Failed to create new lvm thin pool in volume group '" + volumeGroupFull + "'"
        );
    }

    public static synchronized OutputData createThin(
        ExtCmd extCmd,
        String volumeGroup,
        String thinPoolName,
        String vlmId,
        long size,
        String lvmConfig,
        String... additionalParameters
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvcreate",
                lvmConfig,
                additionalParameters,
                "--virtualsize", size + "k", // -V
                volumeGroup,
                "--thinpool", thinPoolName,
                "--name", vlmId        // -n
            ),
            "Failed to create lvm volume",
            "Failed to create new lvm volume '" + vlmId + "' in volume group '" + volumeGroup +
            "' with size " + size + "kb"
        );
    }

    public static synchronized OutputData delete(
        ExtCmd extCmd,
        String volumeGroup,
        String vlmId,
        String lvmConfig,
        LvmVolumeType type
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvremove",
                lvmConfig,
                (Collection<String>) null,
                "-f", // skip the "are you sure?"
                volumeGroup + File.separator + vlmId
            ),
            "Failed to delete lvm " + type.descr,
            "Failed to delete lvm " + type.descr + " '" + vlmId + "' from volume group '" + volumeGroup,
            new RetryIfDeviceBusy()
        );
    }

    public static synchronized OutputData resize(
        ExtCmd extCmd,
        String volumeGroup,
        String vlmId,
        long size,
        String lvmConfig
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvresize",
                lvmConfig,
                (Collection<String>) null,
                "--size", size + "k",
                volumeGroup + File.separator + vlmId,
                "-f"
            ),
            "Failed to resize lvm volume",
            "Failed to resize lvm volume '" + vlmId + "' in volume group '" + volumeGroup + "' to size " + size
        );
    }

    public static synchronized OutputData rename(
        ExtCmd extCmd,
        String volumeGroup,
        String vlmCurrentId,
        String vlmNewId,
        @Nullable String lvmConfig
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvrename",
                lvmConfig,
                (Collection<String>) null,
                volumeGroup,
                vlmCurrentId,
                vlmNewId
            ),
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

                    String err = new String(outData.stderrData);
                    if (err.contains("Volume group \"" + volumeGroup + "\" not found"))
                    {
                        // well - resource is gone... with the whole volume-group
                        skip = true;
                    }
                    return skip;
                }
            }
        );
    }

    public static synchronized OutputData createSnapshot(
        ExtCmd extCmd,
        String volumeGroup,
        String identifier,
        String snapshotIdentifier,
        String lvmConfig,
        long size,
        String... additionalParameters
    )
        throws StorageException
    {
        String failMsg = "Failed to create snapshot " + snapshotIdentifier + " from " + identifier +
            " within volume group " + volumeGroup;
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvcreate",
                lvmConfig,
                additionalParameters == null ? null : Arrays.asList(additionalParameters),
                "--size", size + "k",
                "--snapshot",
                "--setactivationskip", "y", // snapshot needs to be active from the beginning
                "--ignoreactivationskip",
                "--activate", "y",
                "--name", snapshotIdentifier,
                volumeGroup + File.separator + identifier
            ),
            failMsg,
            failMsg
        );
    }

    public static synchronized OutputData createSnapshotThin(
        ExtCmd extCmd,
        boolean readOnly,
        String volumeGroup,
        String thinPool,
        String identifier,
        String snapshotIdentifier,
        String lvmConfig,
        String... additionalParameters
    )
        throws StorageException
    {
        ArrayList<String> addParams = new ArrayList<>(Arrays.asList(additionalParameters));
        if (readOnly)
        {
            addParams.add("-pr");
        }
        String failMsg = "Failed to create snapshot " + snapshotIdentifier + " from " + identifier +
            " within thin volume group " + volumeGroup + File.separator + thinPool;
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvcreate",
                lvmConfig,
                addParams,
                "--snapshot",
                "--setactivationskip", "y", // snapshot needs to be active from
                "--ignoreactivationskip", // the beginning for
                "--activate", "y", // backup-shipping to work
                "--name", snapshotIdentifier,
                volumeGroup + File.separator + identifier
            ),
            failMsg,
            failMsg
        );
    }

    public static synchronized OutputData restoreFromSnapshot(
        ExtCmd extCmd,
        String sourceLvIdWithSnapName,
        String volumeGroup,
        String targetId,
        String lvmConfig,
        String... additionalParameters
    )
        throws StorageException
    {
        String failMsg = "Failed to restore snapshot " + sourceLvIdWithSnapName +
            " into new volume " + volumeGroup + File.separator + targetId;
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvcreate",
                lvmConfig,
                additionalParameters == null ? null : Arrays.asList(additionalParameters),
                "--snapshot",
                "--name", targetId,
                volumeGroup + File.separator + sourceLvIdWithSnapName
            ),
            failMsg,
            failMsg
        );
    }

    public static synchronized OutputData rollbackToSnapshot(
        ExtCmd extCmd,
        String volumeGroup,
        String sourceResource,
        String lvmConfig
    )
        throws StorageException
    {
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvconvert",
                lvmConfig,
                (Collection<String>) null,
                "--merge",
                volumeGroup + File.separator + sourceResource
            ),
            "Failed to rollback to snapshot " + volumeGroup + File.separator + sourceResource,
            "Failed to rollback to snapshot " + volumeGroup + File.separator + sourceResource
        );
    }

    public static synchronized OutputData activateVolume(
        ExtCmd extCmd,
        String volumeGroup,
        String targetId,
        String lvmConfig
    )
        throws StorageException
    {
        String failMsg = "Failed to activate volume " + volumeGroup + File.separator + targetId;
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvchange",
                lvmConfig,
                (Collection<String>) null,
                "-ay",  // activate volume
                "-K",   // these parameters are needed to set a
                // snapshot to active and enabled
                volumeGroup + File.separator + targetId
            ),
            failMsg,
            failMsg
        );
    }

    public static synchronized OutputData deactivateVolume(
        ExtCmd extCmd,
        String volumeGroup,
        String targetId,
        String lvmConfig
    )
        throws StorageException
    {
        String failMsg = "Failed to deactivate volume " + volumeGroup + File.separator + targetId;
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvchange",
                lvmConfig,
                (Collection<String>) null,
                "-an",  // deactivate volume
                volumeGroup + File.separator + targetId
            ),
            failMsg,
            failMsg
        );
    }

    public static synchronized OutputData addTag(
        ExtCmd extCmd, String volumeGroup, String targetId, String tagname, String lvmConfig)
        throws StorageException
    {
        String failMsg = "Failed to set tag on volume " + volumeGroup + File.separator + targetId;
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvchange",
                lvmConfig,
                (Collection<String>) null,
                "--addtag", tagname,
                volumeGroup + File.separator + targetId
            ),
            failMsg,
            failMsg
        );
    }

    public static synchronized OutputData activateZero(
        ExtCmdFactory extCmdFactory,
        String vlmGrp,
        String thinPool,
        String lvmConfig
    )
        throws StorageException
    {
        String failMsg = "Failed to activate --zero y on thin pool: " + vlmGrp + "/" + thinPool;
        return genericExecutor(
            extCmdFactory.create().setSaveWithoutSharedLocks(false),
            buildCmd(
                "lvchange",
                lvmConfig,
            (Collection<String>) null,
            "--zero",
            "y",
            vlmGrp + File.separator + thinPool
        ), failMsg, failMsg);
    }

    public static synchronized OutputData pvCreate(ExtCmd extCmd, String devicePath, String lvmConfig)
        throws StorageException
    {
        final String failMsg = "Failed to pvcreate on device: " + devicePath;
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "pvcreate",
                lvmConfig,
                (Collection<String>) null,
                devicePath
            ),
            failMsg,
            failMsg
        );
    }

    public static synchronized OutputData pvRemove(ExtCmd extCmd, Collection<String> devicePaths, String lvmConfig)
        throws StorageException
    {
        // no lvm config for pvremove!
        final String failMsg = "Failed to pvremove on device(s): " + String.join(", ", devicePaths);
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "pvremove",
                lvmConfig,
                devicePaths
            ),
            failMsg,
            failMsg
        );
    }

    public static synchronized OutputData vgCreate(
        ExtCmd extCmd,
        final String vgName,
        final RaidLevel raidLevel,  // ignore for now as we only support JBOD
        final List<String> devicePaths,
        String lvmConfig
    )
        throws StorageException
    {
        // no lvm config for vgcreate!
        final String failMsg = "Failed to vgcreate on device(s): " + String.join(" ", devicePaths);
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "vgcreate",
                lvmConfig,
                devicePaths,
                vgName
            ),
            failMsg,
            failMsg
        );
    }

    public static synchronized OutputData listPhysicalVolumes(ExtCmd extCmdRef, String volumeGroupRef, String lvmConfig)
        throws StorageException
    {
        final String failMsg = "Failed to get physical devices for volume group: " + volumeGroupRef;
        return genericExecutor(
            extCmdRef.setSaveWithoutSharedLocks(true),
            buildCmd(
                "pvdisplay",
                LVM_CONF_IGNORE_DRBD_DEVICES, // always ignore DRBD devices
                (Collection<String>) null,
                "--columns",
                "-o", "pv_name",
                "-S", "vg_name=" + volumeGroupRef,
                "--noheadings",
                "--nosuffix"
            ),
            failMsg,
            failMsg
        );
    }


    public static synchronized OutputData vgRemove(
        ExtCmd extCmd,
        final String vgName,
        String lvmConfig
    )
        throws StorageException
    {
        // no lvm config for vgremove!
        final String failMsg = "Failed to vgremove on volume group: " + vgName;
        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(false),
            buildCmd(
                "vgremove",
                lvmConfig,
                (Collection<String>) null,
                vgName
            ),
            failMsg,
            failMsg
        );
    }

    public static synchronized OutputData vgscan(ExtCmd extCmd, boolean ignoreCache) throws StorageException
    {
        String[] cmd;
        if (ignoreCache)
        {
            cmd = new String[]
            {
                "vgscan", "-qq", // quite and auto-"no"
                "--cache" // yes, that means to bypass LVMs cache...
            };
        }
        else
        {
            cmd = new String[]
            {
                "vgscan", "-qq" // quite and auto-"no"
            };
        }

        return genericExecutor(
            extCmd.setSaveWithoutSharedLocks(true),
            cmd,
            "'vgscan' failed",
            "'vgscan' failed"
        );
    }

    public enum LvmVolumeType
    {
        VOLUME("volume"), SNAPSHOT("snapshot"), THIN_POOL("thin pool");

        private final String descr;

        LvmVolumeType(String descrRef)
        {
            descr = descrRef;
        }
    }

    private LvmCommands()
    {
    }
}
