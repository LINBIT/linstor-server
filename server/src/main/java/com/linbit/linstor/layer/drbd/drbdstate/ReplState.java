package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.utils.MathUtils;

public enum ReplState
{
    OFF(Consts.RS_LABEL_OFF),
    ESTABLISHED(Consts.RS_LABEL_ESTABLISHED),
    STARTING_SYNC_SOURCE(Consts.RS_LABEL_STARTING_SYNC_SOURCE),
    STARTING_SYNC_TARGET(Consts.RS_LABEL_STARTING_SYNC_TARGET),
    WF_BITMAP_SOURCE(Consts.RS_LABEL_WF_BITMAP_SOURCE), 
    WF_BITMAP_TARGET(Consts.RS_LABEL_WF_BITMAP_TARGET), 
    WF_SYNC_UUID(Consts.RS_LABEL_WF_SYNC_UUID), 
    SYNC_SOURCE(Consts.RS_LABEL_SYNC_SOURCE), 
    SYNC_TARGET(Consts.RS_LABEL_SYNC_TARGET), 
    PAUSED_SYNC_SOURCE(Consts.RS_LABEL_PAUSED_SYNC_SOURCE), 
    PAUSED_SYNC_TARGET(Consts.RS_LABEL_PAUSED_SYNC_TARGET),
    VERIFY_SOURCE(Consts.RS_LABEL_VERIFY_SOURCE), 
    VERIFY_TARGET(Consts.RS_LABEL_VERIFY_TARGET), 
    AHEAD(Consts.RS_LABEL_AHEAD), 
    BEHIND(Consts.RS_LABEL_BEHIND),
    UNKNOWN(Consts.RS_LABEL_UNKNOWN);

    private final String replLabel;

    ReplState(String label)
    {
        replLabel = label;
    }

    public static ReplState parseReplState(String label)
    {
        ReplState result = ReplState.UNKNOWN;
        switch (label)
        {
            case Consts.RS_LABEL_OFF:
                result = ReplState.OFF;
                break;
            case Consts.RS_LABEL_ESTABLISHED:
                result = ReplState.ESTABLISHED;
                break;
            case Consts.RS_LABEL_STARTING_SYNC_SOURCE:
                result = ReplState.STARTING_SYNC_SOURCE;
                break;
            case Consts.RS_LABEL_STARTING_SYNC_TARGET:
                result = ReplState.STARTING_SYNC_TARGET;
                break;
            case Consts.RS_LABEL_WF_BITMAP_SOURCE:
                result = ReplState.WF_BITMAP_SOURCE;
                break;
            case Consts.RS_LABEL_WF_BITMAP_TARGET:
                result = ReplState.WF_BITMAP_TARGET;
                break;
            case Consts.RS_LABEL_WF_SYNC_UUID:
                result = ReplState.WF_SYNC_UUID;
                break;
            case Consts.RS_LABEL_SYNC_SOURCE:
                result = ReplState.SYNC_SOURCE;
                break;
            case Consts.RS_LABEL_SYNC_TARGET:
                result = ReplState.SYNC_TARGET;
                break;
            case Consts.RS_LABEL_PAUSED_SYNC_SOURCE:
                result = ReplState.PAUSED_SYNC_SOURCE;
                break;
            case Consts.RS_LABEL_PAUSED_SYNC_TARGET:
                result = ReplState.PAUSED_SYNC_TARGET;
                break;
            case Consts.RS_LABEL_VERIFY_SOURCE:
                result = ReplState.VERIFY_SOURCE;
                break;
            case Consts.RS_LABEL_VERIFY_TARGET:
                result = ReplState.VERIFY_TARGET;
                break;
            case Consts.RS_LABEL_AHEAD:
                result = ReplState.AHEAD;
                break;
            case Consts.RS_LABEL_BEHIND:
                result = ReplState.BEHIND;
                break;
            case Consts.RS_LABEL_UNKNOWN:
                // fall-through
            default:
                // no-op
                break;
        }
        return result;
    }

    public static float parseDone(String donePerc)
        throws EventsSourceException
    {
        float result;
        try
        {
            result = MathUtils.<Float>bounds((float) 0, Float.parseFloat(donePerc), (float) 100);
        }
        catch (NumberFormatException ignored)
        {
            throw new EventsSourceException(
                "Done percentage is not a parsable number: " + donePerc
            );
        }
        return result;
    }

    @Override
    public String toString()
    {
        return replLabel;
    }

    private static class Consts
    {
        public static final String RS_LABEL_OFF = "Off";
        public static final String RS_LABEL_ESTABLISHED = "Established";
        public static final String RS_LABEL_STARTING_SYNC_SOURCE = "StartingSyncS";
        public static final String RS_LABEL_STARTING_SYNC_TARGET = "StartingSyncT";
        public static final String RS_LABEL_WF_BITMAP_SOURCE = "WFBitMapS";
        public static final String RS_LABEL_WF_BITMAP_TARGET = "WFBitMapT";
        public static final String RS_LABEL_WF_SYNC_UUID = "WFSyncUUID";
        public static final String RS_LABEL_SYNC_SOURCE = "SyncSource";
        public static final String RS_LABEL_SYNC_TARGET = "SyncTarget";
        public static final String RS_LABEL_PAUSED_SYNC_SOURCE = "PausedSyncS";
        public static final String RS_LABEL_PAUSED_SYNC_TARGET = "PausedSyncT";
        public static final String RS_LABEL_VERIFY_SOURCE = "VerifyS";
        public static final String RS_LABEL_VERIFY_TARGET = "VerifyT";
        public static final String RS_LABEL_AHEAD = "Ahead";
        public static final String RS_LABEL_BEHIND = "Behind";
        public static final String RS_LABEL_UNKNOWN = "Unknown";
    }
}
