package com.linbit.drbdmanage.drbdstate;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 *
 * @author Rene Blauensteiner &lt;rene.blauensteiner@linbit.com&gt;
 */
public class EventsTracker
{
    // ===============================================================================================
    // Attributes
    // ===============================================================================================
    /* DEBUG-CONSTANTS */
    // creates
    private static String CREATE_RESOURCE = "create resource name:test role:Secondary suspended:no";
    private static String CREATE_CONNECTION = "create connection name:test peer-node-id:0 conn-name:K14BoxThree connection:StandAlone role:Unknown";
    private static String CREATE_VOLUME = "";
    private static String CREATE_PEER_DEVICE = "create peer-device name:test peer-node-id:0 conn-name:K14BoxThree volume:0 replication:Off peer-disk:DUnknown resync-suspended:no";
    private static String CREATE_DEVICE = "create device name:test volume:0 minor:100 disk:Diskless";
    private static String CREATE_PATH = "create path name:test peer-node-id:0 conn-name:K14BoxThree local:ipv4:10.43.6.149:7000 peer:ipv4:10.43.6.150:7000 established:no";
    // changes
    private static String CHANGE_RESOURCE = "";
    private static String CHANGE_CONNECTION = "change connection name:test peer-node-id:0 conn-name:K14BoxThree connection:TearDown role:Unknown";
    private static String CHANGE_VOLUME = "";
    private static String CHANGE_PEER_DEVICE = "change peer-device name:test peer-node-id:0 conn-name:K14BoxThree volume:0 replication:Established";
    private static String CHANGE_DEVICE = "change device name:test volume:0 minor:100 disk:UpToDate";
    private static String CHANGE_PATH = "change device name:test volume:0 minor:100 disk:UpToDate";
    // deletes
    private static String DESTROY_RESOURCE = "destroy resource name:test";
    private static String DESTROY_CONNECTION = "destroy connection name:test peer-node-id:0 conn-name:K14BoxThree";
    private static String DESTROY_VOLUMEN = "";
    private static String DESTROY_PEER_DEVICE = "destroy peer-device name:test peer-node-id:0 conn-name:K14BoxThree volume:0";
    private static String DESTROY_DEVICE = "destroy device name:test volume:0 minor:100";
    private static String DESTROY_PATH = "";

    /* References */
    private final StateTracker stateTracker;

    // ===============================================================================================
    // Constructor
    // ===============================================================================================
    public EventsTracker()
    {
        this.stateTracker = new StateTracker();
    }

    public EventsTracker(StateTracker stateTracker)
    {
        this.stateTracker = stateTracker;
    }

    //================================================================================================
    // Mechanisms
    //================================================================================================
    public void receiveEvent(String eventString) throws EventsSourceException
    {
        if (eventString != null && !eventString.isEmpty())
        {
            StringTokenizer st = new StringTokenizer(eventString, " ");
            Map<String, String> props = new HashMap<>();
            String action = null;
            String object = null;

            while (st.hasMoreTokens())
            {
                String token = st.nextToken();
                if (token.contains(":"))
                {
                    String nKey = token.substring(0, token.indexOf(":"));
                    String nValue = token.substring(token.indexOf(":") + 1);
                    props.put(nKey, nValue);
                }
                else
                {
                    if (action == null)
                    {
                        action = token;
                    }
                    else if (object == null)
                    {
                        object = token;
                    }
                    else
                    {
                        System.err.println("ups!");
                    }
                }
            }

            // react on the specific action
            switch (action)
            {
                case "create":
                    create(props, object);
                    break;
                case "change":
                    change(props, object);
                    break;
                case "destroy":
                    destroy(props, object);
                    break;
            }
        }
        else
        {
            System.err.println("EventString is null or empty!");
        }
    }

    //================================================================================================
    // Action-Methods
    //================================================================================================
    private void create(Map<String, String> props, String object)
    {
        System.out.println("creating...");
        switch (object)
        {
            case "resource":
                createResource(props);
                break;
            case "connection":
                createConnection(props);
                break;
            case "volume":
                createVolume(props);
                break;
            case "peer-device":
                createPeerDevice(props);
                break;
            case "device":
                createDevice(props);
                break;
            case "path":
                createPath(props);
                break;
            default:
                // FIXME: logging? exception?
                break;
        }
    }

    private void change(Map<String, String> props, String object) throws EventsSourceException
    {
        System.out.println("changing...");
        switch (object)
        {
            case "resource":
                changeResource(props);
                break;
            case "connection":
                changeConnection(props);
                break;
            case "volume":
                changeVolume(props);
                break;
            case "peer-device":
                changePeerDevice(props);
                break;
            case "device":
                changeDevice(props);
                break;
            case "path":
                changePath(props);
                break;
            default:
                // FIXME: logging? exception?
                break;
        }
    }

    private void destroy(Map<String, String> props, String object) throws EventsSourceException
    {
        System.out.println("destroying");
        switch (object)
        {
            case "resource":
                destroyResource(props);
                break;
            case "connection":
                destroyConnection(props);
                break;
            case "volume":
                destroyVolume(props);
                break;
            case "peer-device":
                destroyPeerDevice(props);
                break;
            case "device":
                destroyDevice(props);
                break;
            case "path":
                destroyPath(props);
                break;
            default:
                // FIXME: logging? exception?
        }
    }

    //================================================================================================
    // Object-Methods - CREATE
    //================================================================================================
    private void createResource(Map<String, String> props)
    {
        try
        {
            DrbdResource resource = DrbdResource.newFromProps(props);
            if (resource != null)
            {
                resource.update(props, null);   // FIXME: Observer
            }
            else
            {
                throw new EventsSourceException("Resource is null!");
            }
        }
        catch (EventsSourceException ex)
        {
            System.err.println(ex.getMessage());
        }
    }

    private void createConnection(Map<String, String> props)
    {
        try
        {
            DrbdResource resource = stateTracker.resources.get(props.get("name"));
            if (resource != null)
            {
                DrbdConnection connection = DrbdConnection.newFromProps(resource, props);
                connection.update(props, null);   // FIXME: Observer
            }
            else
            {
                throw new EventsSourceException("Resource is null!");
            }
        }
        catch (EventsSourceException ex)
        {
            System.err.println(ex.getMessage());
        }
    }

    private void createVolume(Map<String, String> props)
    {
        try
        {
            DrbdResource resource = stateTracker.resources.get(props.get("name"));
            if (resource != null)
            {
                DrbdVolume volume = DrbdVolume.newFromProps(resource, props);
                volume.update(props, null);   // FIXME: Observer
            }
            else
            {
                throw new EventsSourceException("Resource is null!");
            }
        }
        catch (EventsSourceException ex)
        {
            System.err.println(ex.getMessage());
        }
    }

    private void createPeerDevice(Map<String, String> props)
    {
        try
        {
            DrbdResource resource = stateTracker.resources.get(props.get("name"));
            if (resource != null)
            {
                DrbdConnection connection = resource.connList.get(props.get("conn-name"));
                if (connection != null)
                {
                    DrbdVolume volume = DrbdVolume.newFromProps(resource, props);
                    connection.volList.put(volume.getVolNr(), volume);
                    volume.update(props, null);   // FIXME! Observer
                }
                else
                {
                    throw new EventsSourceException("Connection is null!");
                }
            }
            else
            {
                throw new EventsSourceException("Resource is null!");
            }
        }
        catch (EventsSourceException ex)
        {
            System.err.println(ex.getMessage());
        }
    }

    private void createDevice(Map<String, String> props)
    {
        // DRBD-Volume
    }

    private void createPath(Map<String, String> props)
    {

    }

    //================================================================================================
    // Object-Methods - CHANGE
    //================================================================================================
    private void changeResource(Map<String, String> props) throws EventsSourceException
    {
        Map<String, DrbdResource> resources = stateTracker.resources;
        DrbdResource resource = resources.get(props.get("name"));
        if (resource != null)
        {
            resource.update(props, null);   // FIXME! Observer
        }
        else
        {
            throw new EventsSourceException("Resource is null!");
        }
    }

    private void changeConnection(Map<String, String> props) throws EventsSourceException
    {
        Map<String, DrbdResource> resources = stateTracker.resources;
        DrbdResource resource = resources.get(props.get("name"));
        if (resource != null)
        {
            DrbdConnection connection = resource.connList.get(props.get("conn-name"));
            if (connection != null)
            {
                connection.update(props, null);   // FIXME! Observer
            }
            else
            {
                throw new EventsSourceException("Connection is null!");
            }
        }
        else
        {
            throw new EventsSourceException("Resource is null!");
        }
    }

    private void changeVolume(Map<String, String> props) throws EventsSourceException
    {
        Map<String, DrbdResource> resources = stateTracker.resources;
        DrbdResource resource = resources.get(props.get("name"));
        if (resource != null)
        {
            DrbdVolume volume = resource.volList.get(props.get("volume"));
            if (volume != null)
            {
                volume.update(props, null);   // FIXME! Observer
            }
            else
            {
                throw new EventsSourceException("Volume is null!");
            }
        }
        else
        {
            throw new EventsSourceException("Resource is null!");
        }
    }

    private void changePeerDevice(Map<String, String> props) throws EventsSourceException
    {
        Map<String, DrbdResource> resources = stateTracker.resources;
        DrbdResource resource = resources.get(props.get("name"));
        if (resource != null)
        {
            DrbdConnection connection = resource.connList.get(props.get("conn-name"));
            if (connection != null)
            {
                DrbdVolume volume = connection.volList.get(props.get("volume"));
                if (volume != null)
                {
                    volume.update(props, null);   // FIXME! Observer
                }
                else
                {
                    throw new EventsSourceException("Volume is null!");
                }
            }
            else
            {
                throw new EventsSourceException("Connection is null!");
            }
        }
        else
        {
            throw new EventsSourceException("Resource is null!");
        }
    }

    private void changeDevice(Map<String, String> props)
    {

    }

    private void changePath(Map<String, String> props)
    {

    }

    //================================================================================================
    // Object-Methods - DESTROY
    //================================================================================================
    private void destroyResource(Map<String, String> props) throws EventsSourceException
    {
        Map<String, DrbdResource> resources = stateTracker.resources;
        DrbdResource resource = resources.get(props.get("name"));
        if (resource != null)
        {
            resource.update(props, null);   // FIXME! Observer
        }
        else
        {
            throw new EventsSourceException("Resource is null!");
        }
    }

    private void destroyConnection(Map<String, String> props) throws EventsSourceException
    {
        Map<String, DrbdResource> resources = stateTracker.resources;
        DrbdResource resource = resources.get(props.get("name"));
        if (resource != null)
        {
            DrbdConnection connection = resource.connList.get(props.get("conn-name"));
            if (connection != null)
            {
                connection.update(props, null);   // FIXME! Observer
            }
            else
            {
                throw new EventsSourceException("Connection is null!");
            }
        }
        else
        {
            throw new EventsSourceException("Resource is null!");
        }
    }

    private void destroyVolume(Map<String, String> props) throws EventsSourceException
    {
        Map<String, DrbdResource> resources = stateTracker.resources;
        DrbdResource resource = resources.get(props.get("name"));
        if (resource != null)
        {
            DrbdVolume volume = resource.volList.get(props.get("volume"));  // props.get("volume") should deliver an integer (Volume-ID)!
            if (volume != null)
            {
                volume.update(props, null);   // FIXME! Observer
            }
            else
            {
                throw new EventsSourceException("Volume is null!");
            }
        }
        else
        {
            throw new EventsSourceException("Resource is null!");
        }
    }

    private void destroyPeerDevice(Map<String, String> props) throws EventsSourceException
    {
        Map<String, DrbdResource> resources = stateTracker.resources;
        DrbdResource resource = resources.get(props.get("name"));
        if (resource != null)
        {
            DrbdConnection connection = resource.connList.get(props.get("conn-name"));
            if (connection != null)
            {
                DrbdVolume volume = connection.volList.get(props.get("volume"));
                if (volume != null)
                {
                    volume.update(props, null);   // FIXME! Observer
                }
                else
                {
                    throw new EventsSourceException("Volume is null!");
                }
            }
            else
            {
                throw new EventsSourceException("Connection is null!");
            }
        }
        else
        {
            throw new EventsSourceException("Resource is null!");
        }
    }

    private void destroyDevice(Map<String, String> props)
    {

    }

    private void destroyPath(Map<String, String> props)
    {

    }

    // ===============================================================================================
    // MAIN
    // ===============================================================================================
    public static void main(String[] args) throws EventsSourceException
    {
        StateTracker st = new StateTracker();
        EventsTracker et = new EventsTracker(st);

//    et.receiveEvent(CREATE_RESOURCE);
//    et.receiveEvent(CREATE_CONNECTION);
//    et.receiveEvent(CREATE_VOLUME);
//    et.receiveEvent(CREATE_PEER_DEVICE);
//    et.receiveEvent(CREATE_DEVICE);
//    et.receiveEvent(CREATE_PATH);
//
//    et.receiveEvent(CHANGE_RESOURCE);
        et.receiveEvent(CHANGE_CONNECTION);
//    et.receiveEvent(CHANGE_VOLUME);
//    et.receiveEvent(CHANGE_PEER_DEVICE);
//    et.receiveEvent(CHANGE_DEVICE);
//    et.receiveEvent(CHANGE_PATH);
//
//    et.receiveEvent(DESTROY_RESOURCE);
//    et.receiveEvent(DESTROY_CONNECTION);
//    et.receiveEvent(DESTROY_VOLUME);
//    et.receiveEvent(DESTROY_PEER_DEVICE);
//    et.receiveEvent(DESTROY_DEVICE);
//    et.receiveEvent(DESTROY_PATH);
    }

}
