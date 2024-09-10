package com.linbit.linstor.layer;

public enum LayerIgnoreReason
{
    NONE("-", false),

    DRBD_DISKLESS("DRBD diskless device"),
    DRBD_SKIP_DISK("DRBD skip-disk", true, true, true),

    EBS_TARGET("EBS target"),
    EBS_MISSING_KEY("EBS no key"),

    LUKS_MISSING_KEY("LUKS no key"),

    NVME_TARGET("NVMe target"),
    NVME_INITIATOR("NVMe initiator"),

    RSC_INACTIVE("Resource inactive"),
    RSC_CLONING("Resource cloning"),

    SED_MISSING_KEY("SED no key"),

    SPDK_TARGET("SPDK target");

    private final String description;
    private final boolean preventExecution;
    private final boolean preventExecutionWhenDeleting;
    private final boolean suppressErrorOnDelete;

    LayerIgnoreReason(String descrRef)
    {
        this(descrRef, true, false, false);
    }

    LayerIgnoreReason(String descrRef, boolean preventExecutionRef)
    {
        this(descrRef, preventExecutionRef, false, false);
    }

    LayerIgnoreReason(
        String descrRef,
        boolean preventExecutionRef,
        boolean preventExecutionWhenDeletingRef,
        boolean suppressErrorOnDeleteRef
    )
    {
        description = descrRef;
        preventExecution = preventExecutionRef;
        preventExecutionWhenDeleting = preventExecutionWhenDeletingRef;
        suppressErrorOnDelete = suppressErrorOnDeleteRef;
    }

    public String getDescription()
    {
        return description;
    }

    public boolean isPreventExecution()
    {
        return preventExecution;
    }

    public boolean isPreventExecutionWhenDeleting()
    {
        return preventExecutionWhenDeleting;
    }

    public boolean isSuppressErrorOnDelete()
    {
        return suppressErrorOnDelete;
    }
}
