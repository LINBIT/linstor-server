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
    public static final String NAMESPACE_NVME = ApiConsts.NAMESPC_STORAGE_DRIVER + "/NVME";

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

    /*
     * ZFS
     */
    public static final String CONFIG_ZFS_POOL_KEY = ApiConsts.KEY_STOR_POOL_NAME;
    public static final String CONFIG_ZFS_THIN_POOL_KEY = ApiConsts.KEY_STOR_POOL_NAME;
    public static final String CONFIG_ZFS_COMMAND_KEY = "zfs";

    public static final Map<String, String> KEY_DESCRIPTION = new HashMap<>();

    /*
     * Openflex
     */
    public static final String CONFIG_OF_API_HOST_KEY = ApiConsts.KEY_STOR_POOL_OPENFLEX_API_HOST;
    public static final String CONFIG_OF_API_PORT_KEY = ApiConsts.KEY_STOR_POOL_OPENFLEX_API_PORT;
    public static final String CONFIG_OF_STOR_DEV_KEY = ApiConsts.KEY_STOR_POOL_OPENFLEX_STOR_DEV;
    public static final String CONFIG_OF_STOR_DEV_HOST_KEY = ApiConsts.KEY_STOR_POOL_OPENFLEX_STOR_DEV_HOST;
    public static final String CONFIG_OF_STOR_POOL_KEY = ApiConsts.KEY_STOR_POOL_NAME;
    public static final String CONFIG_OF_USER_NAME_KEY = ApiConsts.KEY_STOR_POOL_OPENFLEX_USER_NAME;
    public static final String CONFIG_OF_USER_PW_KEY = ApiConsts.KEY_STOR_POOL_OPENFLEX_USER_PW;
    public static final String CONFIG_OF_JOB_WAIT_MAX_COUNT = ApiConsts.KEY_STOR_POOL_OPENFLEX_JOB_WAIT_MAX_COUNT;
    public static final String CONFIG_OF_JOB_WAIT_DELAY = ApiConsts.KEY_STOR_POOL_OPENFLEX_JOB_WAIT_DELAY;

    // internal
    public static final String CONFIG_OF_NQN = "NQN";
    // public static final String CONFIG_OF_POLL_TIMEOUT_VLM_CRT_KEY =
    // ApiConsts.KEY_STOR_POOL_OPENFLEX_POLL_TIMEOUT_VLM_CRT;
    // public static final String CONFIG_OF_POLL_RETRIES_VLM_CRT_KEY =
    // ApiConsts.KEY_STOR_POOL_OPENFLEX_POLL_RETRIES_VLM_CRT;
    // public static final String CONFIG_OF_POLL_TIMEOUT_ATTACH_VLM_KEY =
    // ApiConsts.KEY_STOR_POOL_OPENFLEX_POLL_TIMEOUT_ATTACH_VLM;
    // public static final String CONFIG_OF_POLL_RETRIES_ATTACH_VLM_KEY =
    // ApiConsts.KEY_STOR_POOL_OPENFLEX_POLL_RETRIES_ATTACH_VLM;
    // public static final String CONFIG_OF_POLL_TIMEOUT_GREP_NVME_UUID_KEY =
    // ApiConsts.KEY_STOR_POOL_OPENFLEX_POLL_TIMEOUT_GREP_NVME_UUID;
    // public static final String CONFIG_OF_POLL_RETRIES_GREP_NVME_UUID_KEY =
    // ApiConsts.KEY_STOR_POOL_OPENFLEX_POLL_RETRIES_GREP_NVME_UUID;
    // public static final String CONFIG_OF_COMPOSED_DEVICE_NAME_KEY =
    // ApiConsts.KEY_STOR_POOL_OPENFLEX_COMPOSED_DEVICE_NAME;
    // public static final String CONFIG_OF_RETRY_COUNT_KEY = ApiConsts.KEY_STOR_POOL_OPENFLEX_RETRY_COUNT;
    // public static final String CONFIG_OF_RETRY_DELAY_KEY = ApiConsts.KEY_STOR_POOL_OPENFLEX_RETRY_DELAY;

    // public static final int CONFIG_OF_RETRY_COUNT_DEFAULT = 5;
    // public static final long CONFIG_OF_RETRY_DELAY_DEFAULT = 2000L;

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
