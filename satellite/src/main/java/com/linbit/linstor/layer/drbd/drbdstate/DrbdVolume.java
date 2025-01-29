package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.Checks;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.types.MinorNumber;

import java.util.Map;
import java.util.Objects;

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
    public static final String PROP_KEY_CLIENT       = "client";
    public static final String PROP_KEY_PEER_CLIENT  = "peer-client";
    public static final String PROP_KEY_DONE         = "done";

    protected final VolumeNumber volId;
    protected MinorNumber volMinorNr;
    protected DiskState volDiskState;
    protected ReplState volReplState;
    protected DrbdResource resRef;
    protected DrbdConnection connRef;
    protected Boolean client;
    protected Float donePercentage;

    protected DrbdVolume(DrbdResource resource, DrbdConnection peerConn, VolumeNumber volNr)
        throws ValueOutOfRangeException
    {
        Checks.rangeCheck(volNr.value, VolumeNumber.VOLUME_NR_MIN, VolumeNumber.VOLUME_NR_MAX);
        volId = volNr;
        volMinorNr = null;
        volDiskState = DiskState.UNKNOWN;
        volReplState = ReplState.UNKNOWN;
        resRef = resource;
        connRef = peerConn;
        client = null;
        donePercentage = null;
    }

    public VolumeNumber getVolNr()
    {
        return volId;
    }

    public MinorNumber getMinorNr()
    {
        return volMinorNr;
    }

    public boolean isClient()
    {
        return client != null && client;
    }

    public Boolean getClient()
    {
        return client;
    }

    public DiskState getDiskState()
    {
        return volDiskState;
    }

    public ReplState getReplState()
    {
        return volReplState;
    }

    protected static DrbdVolume newFromProps(
        DrbdResource resource,
        DrbdConnection connection,
        Map<String, String> props
    )
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
            newVolume = new DrbdVolume(resource, connection, new VolumeNumber(volNr));
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
        donePercentage = null;
        String minorNrStr = props.get(PROP_KEY_MINOR);
        String replLabel = props.get(PROP_KEY_REPLICATION);
        String doneLabel = props.get(PROP_KEY_DONE);
        String diskLabel = props.get(PROP_KEY_DISK);
        String clientLabel = props.get(PROP_KEY_CLIENT);
        if (clientLabel == null)
        {
            clientLabel = props.get(PROP_KEY_PEER_CLIENT);
        }

        if (diskLabel == null)
        {
            diskLabel = props.get(PROP_KEY_PEER_DISK);
        }

        if (clientLabel != null)
        {
            client = null;
            if (clientLabel.equals("yes"))
            {
                client = true;
            }
            else
            if (clientLabel.equals("no"))
            {
                client = false;
            }
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

        if (doneLabel != null)
        {
            Float prevPercentage = donePercentage;
            donePercentage = ReplState.parseDone(doneLabel);
            if (!Objects.equals(prevPercentage, donePercentage))
            {
                obs.donePercentageChanged(resRef, null, this, prevPercentage, donePercentage);
            }

            if (volReplState == ReplState.SYNC_TARGET)
            {
                obs.diskStateChanged(resRef, null, this, volDiskState, volDiskState);
            }
        }
        else
        {
            obs.donePercentageChanged(resRef, null, this, donePercentage, null);
            donePercentage = null;
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

    public String diskStateInfo()
    {
        String info = volDiskState.toString();
        if (donePercentage != null && volReplState == ReplState.SYNC_TARGET)
        {
            info = String.format("SyncTarget(%.2f%%)", donePercentage);
        }
        return info;
    }

    public ReplState replicationStateInfo()
    {
        return volReplState;
    }
}
