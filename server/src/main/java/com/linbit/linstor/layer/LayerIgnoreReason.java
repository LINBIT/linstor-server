package com.linbit.linstor.layer;

import java.util.Set;

public enum LayerIgnoreReason
{
    // no need for "none" - the set of ignoreReasons is simply empty in that case
    // NONE("-", false),

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

    public static boolean hasAnyPreventExecutionSet(Set<LayerIgnoreReason> setRef)
    {
        boolean ret = false;
        for (LayerIgnoreReason reason : setRef)
        {
            if (reason.preventExecution)
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    public static boolean hasAnyPreventExecutionWhenDeletingSet(Set<LayerIgnoreReason> ignoreReasonsRef)
    {
        boolean ret = false;
        for (LayerIgnoreReason reason : ignoreReasonsRef)
        {
            if (reason.preventExecutionWhenDeleting)
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    public static boolean hasAnySuppressErrorOnDeleteSet(Set<LayerIgnoreReason> ignoreReasonsRef)
    {
        boolean ret = false;
        for (LayerIgnoreReason reason : ignoreReasonsRef)
        {
            if (reason.suppressErrorOnDelete)
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    public static String getDescriptions(Set<LayerIgnoreReason> ignoreReasonsRef)
    {
        String ret;
        if (ignoreReasonsRef.isEmpty())
        {
            ret = "-";
        }
        else
        {
            StringBuilder sb = new StringBuilder("[ ");
            for (LayerIgnoreReason reason : ignoreReasonsRef)
            {
                sb.append(reason.description).append(", ");
            }
            sb.setLength(sb.length() - 2); // cut the last ", "
            sb.append(" ]");
            ret = sb.toString();
        }
        return ret;
    }

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
