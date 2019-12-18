package com.linbit.linstor.storage;

import com.linbit.linstor.api.ApiConsts;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class StorageConstants
{
    public static final String NAMESPACE_STOR_DRIVER = ApiConsts.NAMESPC_STORAGE_DRIVER;

    /*
     * LVM
     */
    public static final String CONFIG_LVM_VOLUME_GROUP_KEY = ApiConsts.KEY_STOR_POOL_VOLUME_GROUP;
    public static final String CONFIG_LVM_THIN_POOL_KEY = ApiConsts.KEY_STOR_POOL_THIN_POOL;

    public static final String CONFIG_LVM_CREATE_COMMAND_KEY = "lvmCreate";
    public static final String CONFIG_LVM_RESIZE_COMMAND_KEY = "lvmResize";
    public static final String CONFIG_LVM_REMOVE_COMMAND_KEY = "lvmRemove";
    public static final String CONFIG_LVM_CHANGE_COMMAND_KEY = "lvmChange";
    public static final String CONFIG_LVM_CONVERT_COMMAND_KEY = "lvmConvert";
    public static final String CONFIG_LVM_LVS_COMMAND_KEY = "lvmLvs";
    public static final String CONFIG_LVM_VGS_COMMAND_KEY = "lvmVgs";

    public static final String CONFIG_SIZE_ALIGN_TOLERANCE_KEY = "alignmentTolerance";

    /*
     * ZFS
     */
    public static final String CONFIG_ZFS_POOL_KEY = ApiConsts.KEY_STOR_POOL_ZPOOL;
    public static final String CONFIG_ZFS_THIN_POOL_KEY = ApiConsts.KEY_STOR_POOL_ZPOOLTHIN;
    public static final String CONFIG_ZFS_COMMAND_KEY = "zfs";

    public static final Map<String, String> KEY_DESCRIPTION = new HashMap<>();

    /*
     * FILE
     */
    public static final String CONFIG_FILE_DIRECTORY_KEY = ApiConsts.KEY_STOR_POOL_FILE_DIRECTORY;

    static
    {
        KEY_DESCRIPTION.put(CONFIG_LVM_VOLUME_GROUP_KEY, "The volume group the driver should use");
        KEY_DESCRIPTION.put(CONFIG_LVM_CREATE_COMMAND_KEY, "Command to the 'lvcreate' executable");
        KEY_DESCRIPTION.put(CONFIG_LVM_RESIZE_COMMAND_KEY, "Command to the 'lvresize' executable");
        KEY_DESCRIPTION.put(CONFIG_LVM_REMOVE_COMMAND_KEY, "Command to the 'lvremove' executable");
        KEY_DESCRIPTION.put(CONFIG_LVM_CHANGE_COMMAND_KEY, "Command to the 'lvchange' executable");
        KEY_DESCRIPTION.put(CONFIG_LVM_LVS_COMMAND_KEY, "Command to the 'lvs' executable");
        KEY_DESCRIPTION.put(CONFIG_LVM_VGS_COMMAND_KEY, "Command to the 'vgs' executable");
        KEY_DESCRIPTION.put(
            CONFIG_SIZE_ALIGN_TOLERANCE_KEY,
            "Specifies how many times of the extent size the volume's size " +
            "can be larger than specified upon creation."
        );
    }

    private StorageConstants()
    {
    }
}
