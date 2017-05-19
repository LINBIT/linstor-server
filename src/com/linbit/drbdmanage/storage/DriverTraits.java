package com.linbit.drbdmanage.storage;

/**
 * Specifies the keys and values of common StorageDriver traits
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DriverTraits
{
    // Storage provisioning: trait key
    public static final String KEY_PROV         = "provisioning";

    // Unit of smallest allocation
    // The value for this value is a String specifying the size
    // in kiB as a decimal number
    public static final String KEY_ALLOC_UNIT   = "allocation-unit";

    // Fat storage provisioning
    public static final String PROV_FAT         = "fat";
    // Thin storage provisioning
    public static final String PROV_THIN        = "thin";

    private DriverTraits()
    {
    }
}
