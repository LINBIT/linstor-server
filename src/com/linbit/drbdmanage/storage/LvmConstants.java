package com.linbit.drbdmanage.storage;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class LvmConstants
{
    public static final String CONFIG_VOLUME_GROUP_KEY = "volumeGroup";

    public static final String CONFIG_LVM_CREATE_COMMAND_KEY = "lvmCreate";
    public static final String CONFIG_LVM_REMOVE_COMMAND_KEY = "lvmRemove";
    public static final String CONFIG_LVM_CHANGE_COMMAND_KEY = "lvmChange";
    public static final String CONFIG_LVM_LVS_COMMAND_KEY = "lvmLvs";
    public static final String CONFIG_LVM_VGS_COMMAND_KEY = "lvmVgs";

    public static final String CONFIG_SIZE_ALIGN_TOLERANCE_KEY = "alignmentTolerance";


    public static final Map<String, String> KEY_DESCRIPTION = new HashMap<>();

    static
    {
        KEY_DESCRIPTION.put(CONFIG_VOLUME_GROUP_KEY, "The volume group the driver should use");
        KEY_DESCRIPTION.put(CONFIG_LVM_CREATE_COMMAND_KEY, "Command to the 'lvcreate' executable");
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
}
