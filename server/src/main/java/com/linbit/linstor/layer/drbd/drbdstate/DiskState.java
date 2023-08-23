package com.linbit.linstor.layer.drbd.drbdstate;

public enum DiskState
{
    DISKLESS(Consts.DS_LABEL_DISKLESS),
    ATTACHING(Consts.DS_LABEL_ATTACHING),
    DETACHING(Consts.DS_LABEL_DETACHING),
    FAILED(Consts.DS_LABEL_FAILED),
    NEGOTIATING(Consts.DS_LABEL_NEGOTIATING),
    INCONSISTENT(Consts.DS_LABEL_INCONSISTENT),
    OUTDATED(Consts.DS_LABEL_OUTDATED),
    UNKNOWN(Consts.DS_LABEL_UNKNOWN),
    CONSISTENT(Consts.DS_LABEL_CONSISTENT),
    UP_TO_DATE(Consts.DS_LABEL_UP_TO_DATE);

    private String diskLabel;

    DiskState(String label)
    {
        diskLabel = label;
    }

    public static DiskState parseDiskState(String label)
    {
        DiskState result = DiskState.UNKNOWN;
        switch (label)
        {
            case Consts.DS_LABEL_DISKLESS:
                result = DiskState.DISKLESS;
                break;
            case Consts.DS_LABEL_ATTACHING:
                result = DiskState.ATTACHING;
                break;
            case Consts.DS_LABEL_DETACHING:
                result = DiskState.DETACHING;
                break;
            case Consts.DS_LABEL_FAILED:
                result = DiskState.FAILED;
                break;
            case Consts.DS_LABEL_NEGOTIATING:
                result = DiskState.NEGOTIATING;
                break;
            case Consts.DS_LABEL_INCONSISTENT:
                result = DiskState.INCONSISTENT;
                break;
            case Consts.DS_LABEL_OUTDATED:
                result = DiskState.OUTDATED;
                break;
            case Consts.DS_LABEL_CONSISTENT:
                result = DiskState.CONSISTENT;
                break;
            case Consts.DS_LABEL_UP_TO_DATE:
                result = DiskState.UP_TO_DATE;
                break;
            case Consts.DS_LABEL_UNKNOWN:
                // fall-through
            default:
                // no-op
                break;
        }
        return result;
    }

    @Override
    public String toString()
    {
        return diskLabel;
    }

    public String getLabel()
    {
        return diskLabel;
    }

    public boolean oneOf(DiskState... states)
    {
        boolean oneOf = false;
        for (DiskState state : states)
        {
            if (this.equals(state))
            {
                oneOf = true;
                break;
            }
        }
        return oneOf;
    }

    private static class Consts
    {
        public static final String DS_LABEL_DISKLESS = "Diskless";
        public static final String DS_LABEL_ATTACHING = "Attaching";
        public static final String DS_LABEL_DETACHING = "Detaching";
        public static final String DS_LABEL_FAILED = "Failed";
        public static final String DS_LABEL_NEGOTIATING = "Negotiating";
        public static final String DS_LABEL_INCONSISTENT = "Inconsistent";
        public static final String DS_LABEL_OUTDATED = "Outdated";
        public static final String DS_LABEL_UNKNOWN = "DUnknown";
        public static final String DS_LABEL_CONSISTENT = "Consistent";
        public static final String DS_LABEL_UP_TO_DATE = "UpToDate";
    }
}
