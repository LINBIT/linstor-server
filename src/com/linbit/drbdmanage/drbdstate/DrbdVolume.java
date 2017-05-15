package com.linbit.drbdmanage.drbdstate;

import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.VolumeNumber;
import java.util.Map;

/**
 * Tracks the state of a kernel DRBD volume or peer volume
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DrbdVolume
{
    public static final String PROP_KEY_VOL_NR       = "volume";
    public static final String PROP_KEY_MINOR        = "minor";
    public static final String PROP_KEY_DISK         = "disk";
    public static final String PROP_KEY_PEER_DISK    = "peer-disk";
    public static final String PROP_KEY_REPLICATION  = "replication";

    public static final String DS_LABEL_DISKLESS     = "Diskless";
    public static final String DS_LABEL_ATTACHING    = "Attaching";
    public static final String DS_LABEL_DETACHING    = "Detaching";
    public static final String DS_LABEL_FAILED       = "Failed";
    public static final String DS_LABEL_NEGOTIATING  = "Negotiating";
    public static final String DS_LABEL_INCONSISTENT = "Inconsistent";
    public static final String DS_LABEL_OUTDATED     = "Outdated";
    public static final String DS_LABEL_UNKNOWN      = "DUnknown";
    public static final String DS_LABEL_CONSISTENT   = "Consistent";
    public static final String DS_LABEL_UP_TO_DATE   = "UpToDate";

    public static final String RS_LABEL_OFF                  = "Off";
    public static final String RS_LABEL_ESTABLISHED          = "Established";
    public static final String RS_LABEL_STARTING_SYNC_SOURCE = "StartingSyncS";
    public static final String RS_LABEL_STARTING_SYNC_TARGET = "StartingSyncT";
    public static final String RS_LABEL_WF_BITMAP_SOURCE     = "WFBitMapS";
    public static final String RS_LABEL_WF_BITMAP_TARGET     = "WFBitMapT";
    public static final String RS_LABEL_WF_SYNC_UUID         = "WFSyncUUID";
    public static final String RS_LABEL_SYNC_SOURCE          = "SyncSource";
    public static final String RS_LABEL_SYNC_TARGET          = "SyncTarget";
    public static final String RS_LABEL_PAUSED_SYNC_SOURCE   = "PausedSyncS";
    public static final String RS_LABEL_PAUSED_SYNC_TARGET   = "PausedSyncT";
    public static final String RS_LABEL_VERIFY_SOURCE        = "VerifyS";
    public static final String RS_LABEL_VERIFY_TARGET        = "VerifyT";
    public static final String RS_LABEL_AHEAD                = "Ahead";
    public static final String RS_LABEL_BEHIND               = "Behind";
    public static final String RS_LABEL_UNKNOWN              = "Unknown";


    public enum DiskState
    {
        DISKLESS(DS_LABEL_DISKLESS),
        ATTACHING(DS_LABEL_ATTACHING),
        DETACHING(DS_LABEL_DETACHING),
        FAILED(DS_LABEL_FAILED),
        NEGOTIATING(DS_LABEL_NEGOTIATING),
        INCONSISTENT(DS_LABEL_INCONSISTENT),
        OUTDATED(DS_LABEL_OUTDATED),
        UNKNOWN(DS_LABEL_UNKNOWN),
        CONSISTENT(DS_LABEL_CONSISTENT),
        UP_TO_DATE(DS_LABEL_UP_TO_DATE);

        private String diskLabel;

        private DiskState(String label)
        {
            diskLabel = label;
        }

        public static DiskState parseDiskState(String label)
        {
            DiskState result = DiskState.UNKNOWN;
            switch (label)
            {
                case DS_LABEL_DISKLESS:
                    result = DiskState.DISKLESS;
                    break;
                case DS_LABEL_ATTACHING:
                    result = DiskState.ATTACHING;
                    break;
                case DS_LABEL_DETACHING:
                    result = DiskState.DETACHING;
                    break;
                case DS_LABEL_FAILED:
                    result = DiskState.FAILED;
                    break;
                case DS_LABEL_NEGOTIATING:
                    result = DiskState.NEGOTIATING;
                    break;
                case DS_LABEL_INCONSISTENT:
                    result = DiskState.INCONSISTENT;
                    break;
                case DS_LABEL_OUTDATED:
                    result = DiskState.OUTDATED;
                    break;
                case DS_LABEL_CONSISTENT:
                    result = DiskState.CONSISTENT;
                    break;
                case DS_LABEL_UP_TO_DATE:
                    result = DiskState.UP_TO_DATE;
                    break;
                case DS_LABEL_UNKNOWN:
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
    }

    public enum ReplState
    {
        OFF(RS_LABEL_OFF),
        ESTABLISHED(RS_LABEL_ESTABLISHED),
        STARTING_SYNC_SOURCE(RS_LABEL_STARTING_SYNC_SOURCE),
        STARTING_SYNC_TARGET(RS_LABEL_STARTING_SYNC_TARGET),
        WF_BITMAP_SOURCE(RS_LABEL_WF_BITMAP_SOURCE),
        WF_BITMAP_TARGET(RS_LABEL_WF_BITMAP_TARGET),
        WF_SYNC_UUID(RS_LABEL_WF_SYNC_UUID),
        SYNC_SOURCE(RS_LABEL_SYNC_SOURCE),
        SYNC_TARGET(RS_LABEL_SYNC_TARGET),
        PAUSED_SYNC_SOURCE(RS_LABEL_PAUSED_SYNC_SOURCE),
        PAUSED_SYNC_TARGET(RS_LABEL_PAUSED_SYNC_TARGET),
        VERIFY_SOURCE(RS_LABEL_VERIFY_SOURCE),
        VERIFY_TARGET(RS_LABEL_VERIFY_TARGET),
        AHEAD(RS_LABEL_AHEAD),
        BEHIND(RS_LABEL_BEHIND),
        UNKNOWN(RS_LABEL_UNKNOWN);

        private String replLabel;

        private ReplState(String label)
        {
            replLabel = label;
        }

        public static ReplState parseReplState(String label)
        {
            ReplState result = ReplState.UNKNOWN;
            switch (label)
            {
                case RS_LABEL_OFF:
                    result = ReplState.OFF;
                    break;
                case RS_LABEL_ESTABLISHED:
                    result = ReplState.ESTABLISHED;
                    break;
                case RS_LABEL_STARTING_SYNC_SOURCE:
                    result = ReplState.STARTING_SYNC_SOURCE;
                    break;
                case RS_LABEL_STARTING_SYNC_TARGET:
                    result = ReplState.STARTING_SYNC_TARGET;
                    break;
                case RS_LABEL_WF_BITMAP_SOURCE:
                    result = ReplState.WF_BITMAP_SOURCE;
                    break;
                case RS_LABEL_WF_BITMAP_TARGET:
                    result = ReplState.WF_BITMAP_TARGET;
                    break;
                case RS_LABEL_WF_SYNC_UUID:
                    result = ReplState.WF_SYNC_UUID;
                    break;
                case RS_LABEL_SYNC_SOURCE:
                    result = ReplState.SYNC_SOURCE;
                    break;
                case RS_LABEL_SYNC_TARGET:
                    result = ReplState.SYNC_TARGET;
                    break;
                case RS_LABEL_PAUSED_SYNC_SOURCE:
                    result = ReplState.PAUSED_SYNC_SOURCE;
                    break;
                case RS_LABEL_PAUSED_SYNC_TARGET:
                    result = ReplState.PAUSED_SYNC_TARGET;
                    break;
                case RS_LABEL_VERIFY_SOURCE:
                    result = ReplState.VERIFY_SOURCE;
                    break;
                case RS_LABEL_VERIFY_TARGET:
                    result = ReplState.VERIFY_TARGET;
                    break;
                case RS_LABEL_AHEAD:
                    result = ReplState.AHEAD;
                    break;
                case RS_LABEL_BEHIND:
                    result = ReplState.BEHIND;
                    break;
                case RS_LABEL_UNKNOWN:
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
            return replLabel;
        }
    }

    protected final VolumeNumber volId;
    protected MinorNumber volMinorNr;
    protected DiskState volDiskState;
    protected ReplState volReplState;
    protected DrbdResource resRef;
    protected DrbdConnection connRef;

    protected DrbdVolume(DrbdResource resource, VolumeNumber volNr)
        throws ValueOutOfRangeException
    {
        this(resource, null, volNr);
    }

    protected DrbdVolume(DrbdResource resource, DrbdConnection peerConn, VolumeNumber volNr)
    {
        volId = volNr;
        volMinorNr = null;
        volDiskState = DiskState.UNKNOWN;
        volReplState = ReplState.UNKNOWN;
        resRef = resource;
        connRef = peerConn;
    }

    public VolumeNumber getVolNr()
    {
        return volId;
    }

    public MinorNumber getMinorNr()
    {
        return volMinorNr;
    }

    public DiskState getDiskState()
    {
        return volDiskState;
    }

    public ReplState getReplState()
    {
        return volReplState;
    }

    protected static DrbdVolume newFromProps(DrbdResource resource, Map<String, String> props)
        throws EventsSourceException
    {
        String volNrStr = props.get(PROP_KEY_VOL_NR);
        if (volNrStr == null)
        {
            throw new EventsSourceException(
                "Create volume event without a volume number"
            );
        }

        int volNr = -1;
        try
        {
            volNr = Integer.parseInt(volNrStr);
        }
        catch (NumberFormatException nfExc)
        {
            throw new EventsSourceException(
                "Create volume event with an unparsable volume number",
                nfExc
            );
        }

        DrbdVolume newVolume;
        try
        {
            newVolume = new DrbdVolume(resource, new VolumeNumber(volNr));
        }
        catch (ValueOutOfRangeException rangeExc)
        {
            throw new EventsSourceException(
                "Create volume event with an invalid volume number",
                rangeExc
            );
        }

        return newVolume;
    }

    public DrbdResource getResource()
    {
        return resRef;
    }

    public DrbdConnection getConnection()
    {
        return connRef;
    }

    protected void setConnection(DrbdConnection conn)
    {
        connRef = conn;
    }

    protected void update(Map<String, String> props, ResourceObserver obs)
        throws EventsSourceException
    {
        String minorNrStr = props.get(PROP_KEY_MINOR);
        String replLabel = props.get(PROP_KEY_REPLICATION);
        String diskLabel = props.get(PROP_KEY_DISK);
        if (diskLabel == null)
        {
            diskLabel = props.get(PROP_KEY_PEER_DISK);
        }

        if (minorNrStr != null)
        {
            MinorNumber prevMinorNr = volMinorNr;
            int minorNr = -1;
            try
            {
                minorNr = Integer.parseInt(minorNrStr);
            }
            catch (NumberFormatException nfExc)
            {
                throw new EventsSourceException(
                    "Event line with unparsable minor number"
                );
            }

            try
            {
                volMinorNr = new MinorNumber(minorNr);
            }
            catch (ValueOutOfRangeException valExc)
            {
                throw new EventsSourceException(
                    "Event line with invalid minor number",
                    valExc
                );
            }
            if (!volMinorNr.equals(prevMinorNr))
            {
                // Only local minor number changes are tracked
                // Peer minor number changes do not normally trigger a local event,
                // the check is just an additional safeguard
                if (connRef == null)
                {
                    obs.minorNrChanged(resRef, this, prevMinorNr, volMinorNr);
                }
            }
        }

        if (replLabel != null)
        {
            ReplState prevReplState = volReplState;
            volReplState = ReplState.parseReplState(replLabel);
            if (prevReplState != volReplState)
            {
                obs.replicationStateChanged(resRef, connRef, this, prevReplState, volReplState);
            }
        }

        if (diskLabel != null)
        {
            DiskState prevDiskState = volDiskState;
            volDiskState = DiskState.parseDiskState(diskLabel);
            if (prevDiskState != volDiskState)
            {
                obs.diskStateChanged(resRef, connRef, this, prevDiskState, volDiskState);
            }
        }
    }
}
