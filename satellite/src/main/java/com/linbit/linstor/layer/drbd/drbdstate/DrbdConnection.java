package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.VolumeNumber;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tracks the state of a kernel DRBD peer connection
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DrbdConnection
{
    public static final String PROP_KEY_CONNECTION   = "connection";
    public static final String PROP_KEY_CONN_NAME    = "conn-name";
    public static final String PROP_KEY_PEER_NODE_ID = "peer-node-id";

    public static final String CS_LABEL_STANDALONE      = "StandAlone";
    public static final String CS_LABEL_DISCONNECTING   = "Disconnecting";
    public static final String CS_LABEL_UNCONNECTED     = "Unconnected";
    public static final String CS_LABEL_TIMEOUT         = "Timeout";
    public static final String CS_LABEL_BROKEN_PIPE     = "BrokenPipe";
    public static final String CS_LABEL_NETWORK_FAILURE = "NetworkFailure";
    public static final String CS_LABEL_PROTOCOL_ERROR  = "ProtocolError";
    public static final String CS_LABEL_CONNECTING      = "Connecting";
    public static final String CS_LABEL_TEAR_DOWN       = "TearDown";
    public static final String CS_LABEL_CONNECTED       = "Connected";
    public static final String CS_LABEL_UNKNOWN         = "Unknown";

    public enum State
    {
        STANDALONE(CS_LABEL_STANDALONE),
        DISCONNECTING(CS_LABEL_DISCONNECTING),
        UNCONNECTED(CS_LABEL_UNCONNECTED),
        TIMEOUT(CS_LABEL_TIMEOUT),
        BROKEN_PIPE(CS_LABEL_BROKEN_PIPE),
        NETWORK_FAILURE(CS_LABEL_NETWORK_FAILURE),
        PROTOCOL_ERROR(CS_LABEL_PROTOCOL_ERROR),
        TEAR_DOWN(CS_LABEL_TEAR_DOWN),
        CONNECTING(CS_LABEL_CONNECTING),
        CONNECTED(CS_LABEL_CONNECTED),
        UNKNOWN(CS_LABEL_UNKNOWN);

        private String stateLabel;

        State(String label)
        {
            stateLabel = label;
        }

        public static State parseState(String label)
        {
            State result = State.UNKNOWN;
            switch (label)
            {
                case CS_LABEL_STANDALONE:
                    result = State.STANDALONE;
                    break;
                case CS_LABEL_DISCONNECTING:
                    result = State.DISCONNECTING;
                    break;
                case CS_LABEL_UNCONNECTED:
                    result = State.UNCONNECTED;
                    break;
                case CS_LABEL_TIMEOUT:
                    result = State.TIMEOUT;
                    break;
                case CS_LABEL_BROKEN_PIPE:
                    result = State.BROKEN_PIPE;
                    break;
                case CS_LABEL_NETWORK_FAILURE:
                    result = State.NETWORK_FAILURE;
                    break;
                case CS_LABEL_PROTOCOL_ERROR:
                    result = State.PROTOCOL_ERROR;
                    break;
                case CS_LABEL_TEAR_DOWN:
                    result = State.TEAR_DOWN;
                    break;
                case CS_LABEL_CONNECTING:
                    result = State.CONNECTING;
                    break;
                case CS_LABEL_CONNECTED:
                    result = State.CONNECTED;
                    break;
                case CS_LABEL_UNKNOWN:
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
            return stateLabel;
        }
    }

    protected final String peerName;
    protected final int peerNodeId;
    protected DrbdResource.Role peerResRole;
    protected State connState;
    protected DrbdResource resRef;
    private final Map<VolumeNumber, DrbdVolume> volList;

    protected DrbdConnection(DrbdResource resource, String connName, int nodeId)
    {
        peerName = connName;
        peerNodeId = nodeId;
        peerResRole = DrbdResource.Role.UNKNOWN;
        connState = State.UNKNOWN;
        volList = new TreeMap<>();
        resRef = resource;
    }

    public String getConnectionName()
    {
        return peerName;
    }

    public int getPeerNodeId()
    {
        return peerNodeId;
    }

    public DrbdResource.Role getPeerRole()
    {
        return peerResRole;
    }

    public State getState()
    {
        return connState;
    }

    protected static DrbdConnection newFromProps(DrbdResource resource, Map<String, String> props)
        throws EventsSourceException
    {
        String connName = props.get(PROP_KEY_CONN_NAME);
        if (connName == null)
        {
            throw new EventsSourceException(
                "Create connection event without a connection name"
            );
        }

        String nodeIdStr = props.get(PROP_KEY_PEER_NODE_ID);
        if (nodeIdStr == null)
        {
            throw new EventsSourceException(
                "Create connection event without a peer node id"
            );
        }

        int nodeId = -1;
        try
        {
            nodeId = Integer.parseInt(nodeIdStr);
        }
        catch (NumberFormatException nfExc)
        {
            throw new EventsSourceException(
                "Create connection event with an unparsable node id",
                nfExc
            );
        }

        return new DrbdConnection(resource, connName, nodeId);
    }

    protected void update(Map<String, String> props, ResourceObserver obs)
    {
        String connLabel = props.get(PROP_KEY_CONNECTION);
        if (connLabel != null)
        {
            State prevConnState = connState;
            connState = State.parseState(connLabel);
            if (prevConnState != connState)
            {
                obs.connectionStateChanged(resRef, this, prevConnState, connState);
            }
        }

        String roleLabel = props.get(DrbdResource.PROP_KEY_ROLE);
        if (roleLabel != null)
        {
            DrbdResource.Role prevRole = peerResRole;
            peerResRole = DrbdResource.Role.parseRole(roleLabel);
            if (prevRole != peerResRole)
            {
                obs.peerRoleChanged(resRef, this, prevRole, peerResRole);
            }
        }
    }

    public DrbdResource getResource()
    {
        return resRef;
    }

    public Iterator<DrbdVolume> iterateVolumes()
    {
        LinkedList<DrbdVolume> volListCopy = new LinkedList<>();
        synchronized (volList)
        {
            volListCopy.addAll(volList.values());
        }
        return volListCopy.iterator();
    }

    public @Nullable DrbdVolume getVolume(VolumeNumber volNr)
    {
        DrbdVolume vol = null;
        synchronized (volList)
        {
            vol = volList.get(volNr);
        }
        return vol;
    }

    void putVolume(DrbdVolume volume)
    {
        synchronized (volList)
        {
            volList.put(volume.volId, volume);
        }
    }

    @Nullable
    DrbdVolume removeVolume(VolumeNumber volNr)
    {
        DrbdVolume removedVol = null;
        synchronized (volList)
        {
            removedVol = volList.remove(volNr);
        }
        return removedVol;
    }
}
