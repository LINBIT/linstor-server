package com.linbit.linstor.storage;

import com.linbit.linstor.api.ApiConsts;

import java.util.HashMap;
import java.util.Map;

public class StorageConstants
{
    public static final String NAMESPACE_STOR_DRIVER = ApiConsts.NAMESPC_STORAGE_DRIVER;
    public static final String NAMESPACE_NVME = ApiConsts.NAMESPC_STORAGE_DRIVER + "/NVME";
    public static final String NAMESPACE_INTERNAL = NAMESPACE_STOR_DRIVER + "/internal/";

    public static final String BLK_DEV_MIN_IO_SIZE      = "minIoSize";
    public static final String BLK_DEV_MIN_IO_SIZE_AUTO = "minIoSizeAuto";
    public static final String BLK_DEV_MAX_BIO_SIZE     = "maxBioSize";

    /*
     * LVM
     */
    public static final String CONFIG_LVM_VOLUME_GROUP_KEY = ApiConsts.KEY_STOR_POOL_NAME;
    public static final String CONFIG_LVM_THIN_POOL_KEY = ApiConsts.KEY_STOR_POOL_NAME;

    public static final String CONFIG_LVM_CREATE_COMMAND_KEY = "lvmCreate";
    public static final String CONFIG_LVM_RESIZE_COMMAND_KEY = "lvmResize";
    public static final String CONFIG_LVM_REMOVE_COMMAND_KEY = "lvmRemove";
    public static final String CONFIG_LVM_CHANGE_COMMAND_KEY = "lvmChange";
    public static final String CONFIG_LVM_CONVERT_COMMAND_KEY = "lvmConvert";
    public static final String CONFIG_LVM_LVS_COMMAND_KEY = "lvmLvs";
    public static final String CONFIG_LVM_VGS_COMMAND_KEY = "lvmVgs";

    public static final String CONFIG_SIZE_ALIGN_TOLERANCE_KEY = "alignmentTolerance";

    public static final String KEY_INT_THIN_POOL_GRANULARITY = "lvmthin/thinPoolGranularity";
    public static final String KEY_INT_THIN_POOL_METADATA_PERCENT = "lvmthin/thinPoolMetadataPercent";
    /*
     * ZFS
     */
    public static final String CONFIG_ZFS_POOL_KEY = ApiConsts.KEY_STOR_POOL_NAME;
    public static final String CONFIG_ZFS_THIN_POOL_KEY = ApiConsts.KEY_STOR_POOL_NAME;
    public static final String CONFIG_ZFS_COMMAND_KEY = "zfs";

    public static final Map<String, String> KEY_DESCRIPTION = new HashMap<>();

    /*
     * Remote SPDK
     */
    public static final String CONFIG_REMOTE_SPDK_API_HOST_KEY = ApiConsts.KEY_STOR_POOL_REMOTE_SPDK_API_HOST;
    public static final String CONFIG_REMOTE_SPDK_API_PORT_KEY = ApiConsts.KEY_STOR_POOL_REMOTE_SPDK_API_PORT;
    public static final String CONFIG_REMOTE_SPDK_USER_NAME_KEY = ApiConsts.KEY_STOR_POOL_REMOTE_SPDK_API_USER_NAME;
    public static final String CONFIG_REMOTE_SPDK_USER_PW_KEY = ApiConsts.KEY_STOR_POOL_REMOTE_SPDK_API_USER_PW;
    public static final String CONFIG_REMOTE_SPDK_USER_NAME_ENV_KEY = ApiConsts.KEY_STOR_POOL_REMOTE_SPDK_API_USER_NAME_ENV;
    public static final String CONFIG_REMOTE_SPDK_USER_PW_ENV_KEY = ApiConsts.KEY_STOR_POOL_REMOTE_SPDK_API_USER_PW_ENV;

    /*
     * FILE
     */
    public static final String CONFIG_FILE_DIRECTORY_KEY = ApiConsts.KEY_STOR_POOL_NAME;

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
