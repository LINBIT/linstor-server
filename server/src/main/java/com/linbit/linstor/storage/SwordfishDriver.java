package com.linbit.linstor.storage;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.utils.Crypt;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestClient;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import com.linbit.linstor.timer.CoreTimer;

import com.linbit.linstor.storage.utils.RestResponse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.jr.ob.JSON;
import com.fasterxml.jackson.jr.ob.impl.CollectionBuilder;
import com.fasterxml.jackson.jr.ob.impl.MapBuilder;

public class SwordfishDriver implements StorageDriver
{
    private static final Pattern PATTERN_NQN = Pattern.compile(
        "^/sys/devices/virtual/nvme-fabrics/ctl/nvme(\\d+)/subsysnqn",
        Pattern.DOTALL | Pattern.MULTILINE
    );

    private static final File SF_MAPPING_FILE = new File("/var/lib/linstor/swordfish.json");
    private static final File SF_MAPPING_FILE_TMP = new File("/var/lib/linstor/swordfish.json.tmp");

    public static final String ODATA = "@odata";
    public static final String JSON_KEY_ODATA_ID = ODATA + ".id";
    public static final String JSON_KEY_MESSAGE = "Message";
    public static final String JSON_KEY_CAPACITY = "Capacity";
    public static final String JSON_KEY_CAPACITY_SOURCES = "CapacitySources";
    public static final String JSON_KEY_PROVIDING_POOLS = "ProvidingPools";
    public static final String JSON_KEY_NAME = "Name";
    public static final String JSON_KEY_CAPACITY_BYTES = "CapacityBytes";
    public static final String JSON_KEY_FREE_SIZE = "AvailableBytes";
    public static final String JSON_KEY_ALLOCATED_BYTES = "AllocatedBytes";
    public static final String JSON_KEY_GUARANTEED_BYTES = "GuaranteedBytes";
    public static final String JSON_KEY_RESOURCE = "Resource";
    public static final String JSON_KEY_DATA = "Data";
    public static final String JSON_KEY_IDENTIFIERS = "Identifiers";
    public static final String JSON_KEY_DURABLE_NAME_FORMAT = "DurableNameFormat";
    public static final String JSON_KEY_DURABLE_NAME = "DurableName";
    public static final Object JSON_KEY_LINKS = "Links";
    public static final Object JSON_KEY_OEM = "Oem";
    public static final Object JSON_KEY_INTEL_RACK_SCALE = "Intel_RackScale";
    public static final Object JSON_KEY_ENDPOINTS = "Endpoints";
    public static final Object JSON_KEY_PARAMETERS = "Parameters";
    public static final Object JSON_KEY_ALLOWABLE_VALUES = "AllowableValues";
    public static final String JSON_VALUE_DURABLE_NAME_FORMAT_SYSTEM_PATH = "SystemPath";
    public static final Object JSON_VALUE_NQN = "NQN";

    public static final String SF_BASE = "/redfish/v1";
    public static final String SF_STORAGE_SERVICES = "/StorageServices";
    public static final String SF_STORAGE_POOLS = "/StoragePools";
    public static final String SF_VOLUMES = "/Volumes";
    public static final String SF_NODES = "/Nodes";
    public static final String SF_ACTIONS = "/Actions";
    public static final String SF_ALLOCAT = "/Allocate";
    public static final String SF_FABRICS = "/Fabrics";
    public static final String SF_ENDPOINTS = "/Endpoints";
    public static final String SF_TASK_SERVICE = "/TaskService";
    public static final String SF_TASKS = "/Tasks";
    public static final String SF_MONITOR = "/Monitor";
    public static final String SF_METRICS = "/Metrics";
    public static final String SF_SYSTEMS = "/Systems";
    public static final String SF_ETHERNET_INTERFACES = "/EthernetInterfaces";
    public static final String SF_ZONES = "/Zones";
    public static final String SF_COMPOSED_NODE_ATTACH_RESOURCE = "/ComposedNode.AttachResource";
    public static final String SF_COMPOSED_NODE_DETACH_RESOURCE = "/ComposedNode.DetachResource";
    public static final String SF_ATTACH_RESOURCE_ACTION_INFO = "/AttachResourceActionInfo";

    public static final String SF_NODES_ACTION_ALLOCAT = SF_BASE + SF_NODES + SF_ACTIONS + SF_ALLOCAT;

    private static final long KIB = 1024;

    private static final Map<String, Object> JSON_OBJ;


    private final ErrorReporter errorReporter;
    private final SwordfishDriverKind swordfishDriverKind;

    private Map<String, String> linstorIdToSwordfishId;

    private String hostPort;
    private String storSvc;
    private String storPool;
    private String userName;
    private String userPw;
    private long pollTimeout = 500;
    private String composedNodeName;

    private final RestClient restClient;
    private final Crypt crypt;
    private final CoreTimer timer;

    static
    {
        try
        {
            Path jsonPath = SF_MAPPING_FILE.toPath();
            if (!Files.exists(jsonPath))
            {
                Files.createDirectories(jsonPath.getParent());
                Files.createFile(jsonPath);
                Files.write(jsonPath, "{}".getBytes());  // otherwise jackson complains about
                // no json content
            }
            JSON_OBJ = JSON.std.mapFrom(SF_MAPPING_FILE);
        }
        catch (IOException exc)
        {
            throw new LinStorRuntimeException("Failed to load swordfish.json", exc);
        }
    }

    public SwordfishDriver(
        ErrorReporter errorReporterRef,
        SwordfishDriverKind swordfishDriverKindRef,
        RestClient restClientRef,
        CoreTimer timerRef,
        Crypt cryptRef
    )
    {
        errorReporter = errorReporterRef;
        swordfishDriverKind = swordfishDriverKindRef;
        restClient = restClientRef;
        timer = timerRef;
        crypt = cryptRef;
    }

    @Override
    public StorageDriverKind getKind()
    {
        return swordfishDriverKind;
    }

    @Override
    public void startVolume(String identifier, String cryptKey) throws StorageException
    {
        if (cryptKey != null)
        {
            crypt.openCryptDevice(
                getVolumePath(identifier, false),
                identifier,
                cryptKey.getBytes()
            );
        }
    }

    @Override
    public void stopVolume(String identifier, boolean isEncrypted) throws StorageException
    {
        if (isEncrypted)
        {
            crypt.closeCryptDevice(identifier);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public String createVolume(String identifier, long size, String cryptKey)
        throws StorageException, MaxSizeException, MinSizeException
    {
        String volumePath;
        try
        {
            // create volume
            // POST to volumes collection
            RestResponse<Map<String, Object>> crtVlmResp = restClient.execute(
                RestOp.POST,
                hostPort + SF_BASE + SF_STORAGE_SERVICES + "/" + storSvc + SF_VOLUMES,
                getDefaultHeader().build(),
                MapBuilder.defaultImpl().start()
                    .put(JSON_KEY_CAPACITY_BYTES, size * KIB)
                    .put(
                        JSON_KEY_CAPACITY_SOURCES,
                        CollectionBuilder.defaultImpl().start()
                            .add(
                                MapBuilder.defaultImpl().start()
                                    .put(
                                        JSON_KEY_PROVIDING_POOLS,
                                        CollectionBuilder.defaultImpl().start()
                                            .add(
                                                MapBuilder.defaultImpl().start()
                                                    .put(
                                                        JSON_KEY_ODATA_ID,
                                                        SF_BASE + SF_STORAGE_SERVICES + "/" + storSvc +
                                                            SF_STORAGE_POOLS + "/" + storPool
                                                    )
                                                    .build()
                                            )
                                            .buildArray()
                                    )
                                    .build()
                            )
                            .buildArray()
                    )
                    .build()
            );
            // volume should be now in "creating" state. we have to wait for the taskMonitor to return HTTP_CREATED

            String taskMonitorLocation = crtVlmResp.getHeaders().get(HttpHeader.LOCATION_KEY);
            // taskLocation should start with SF_BASE + SF_TASK_SERVICE + SF_TASKS followed by "/$taskId/Monitor"
            String taskId;
            try
            {
                taskId = taskMonitorLocation.substring(
                    (SF_BASE + SF_TASK_SERVICE + SF_TASKS + "/").length(),
                    taskMonitorLocation.indexOf(SF_MONITOR)
                );
            }
            catch (NumberFormatException | ArrayIndexOutOfBoundsException exc)
            {
                throw new StorageException(
                    "Task-id could not be parsed. Task monitor url: " + taskMonitorLocation,
                    exc
                );
            }

            errorReporter.logTrace(
                "volume creation response status code: %d, taskId: %s",
                crtVlmResp.getStatusCode(),
                taskId
            );
            String vlmLocation = null;
            while (vlmLocation == null)
            {
                errorReporter.logTrace("waiting %d ms before polling task monitor", pollTimeout);
                Thread.sleep(pollTimeout);

                RestResponse<Map<String, Object>> crtVlmTaskResp = restClient.execute(
                    RestOp.GET,
                    hostPort  + taskMonitorLocation,
                    getDefaultHeader().noContentType().build(),
                    (String) null
                );
                switch (crtVlmTaskResp.getStatusCode())
                {
                    case HttpHeader.HTTP_ACCEPTED: // noop, task is still in progress
                        break;
                    case HttpHeader.HTTP_CREATED: // task created successfully
                        vlmLocation = crtVlmTaskResp.getHeaders().get(HttpHeader.LOCATION_KEY);
                        break;
                    default: // problem
                        throw new StorageException(
                            String.format(
                                "Unexpected return code from task monitor %s: %d",
                                taskMonitorLocation,
                                crtVlmTaskResp.getStatusCode()
                            )
                        );
                }
            }
            // volume created
            // extract the swordfish id of that volume and persist if for later lookups
            String sfId = vlmLocation.substring(
                (SF_BASE + SF_STORAGE_SERVICES + "/" + storSvc + SF_VOLUMES + "/").length()
            );

            // extract volume's @odata.id
            // RestResponse<Map<String, Object>> vlmDataResp = restClient.execute(
            //     RestOp.GET,
            //     hostPort + vlmLocation,
            //     getDefaultHeader().build(),
            //     null
            // );
            String vlmOdataId = vlmLocation;


            errorReporter.logTrace("volume created with swordfish id: %s, @odata.id: %s", sfId, vlmOdataId);
            linstorIdToSwordfishId.put(identifier, sfId);
            persistJson();

            // volume is create but might not be ready to be attached.
            // wait until volume shows up in node's Actions/AttachResourceActionInfo
            String attachInfoAction = SF_BASE + SF_NODES + "/" + composedNodeName + SF_ACTIONS +
                SF_ATTACH_RESOURCE_ACTION_INFO;
            boolean attachable = false;

            int failCount = 0;
            while (!attachable)
            {
                errorReporter.logTrace("waiting %d ms before polling node's Actions/AttachResourceActionInfo", pollTimeout);
                Thread.sleep(pollTimeout);

                RestResponse<Map<String, Object>> attachRscInfoResp = restClient.execute(
                    RestOp.GET,
                    hostPort  + attachInfoAction,
                    getDefaultHeader().noContentType().build(),
                    (String) null
                );
                if (attachRscInfoResp.getStatusCode() != HttpHeader.HTTP_OK)
                {
                    throw new StorageException(
                        String.format(
                            "Unexpected return code (%d) from %s",
                            attachRscInfoResp.getStatusCode(),
                            attachInfoAction
                        )
                    );
                }
                Map<String, Object> attachRscInfoData = attachRscInfoResp.getData();
                ArrayList<Object> attachInfoParameters = (ArrayList<Object>) attachRscInfoData.get(JSON_KEY_PARAMETERS);
                for (Object attachInfoParameter : attachInfoParameters)
                {
                    Map<String, Object> attachInfoParamMap = (Map<String, Object>) attachInfoParameter;
                    ArrayList<Object> paramAllowableValues = (ArrayList<Object>) attachInfoParamMap.get(JSON_KEY_ALLOWABLE_VALUES);
                    if (paramAllowableValues != null)
                    {
                        for (Object paramAllowableValue : paramAllowableValues)
                        {
                            if (paramAllowableValue instanceof Map)
                            {
                                Map<String, Object> paramAllowableValueMap = (Map<String, Object>) paramAllowableValue;
                                String attachableVlmId = (String) paramAllowableValueMap.get(JSON_KEY_ODATA_ID);
                                if (vlmOdataId.equals(attachableVlmId))
                                {
                                    attachable = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                if (failCount++ >= 300)
                {
                    throw new StorageException(
                        "Volume is not attachable. node's ActionInfo: " + attachInfoAction + ". volume id: " + vlmOdataId
                    );
                }
            }

            // attach the volume to the composed node
            String attachAction = SF_BASE + SF_NODES + "/" + composedNodeName + SF_ACTIONS +
                SF_COMPOSED_NODE_ATTACH_RESOURCE;
            RestResponse<Map<String, Object>> attachVlmResp = restClient.execute(
                RestOp.POST,
                hostPort + attachAction,
                getDefaultHeader().build(),
                MapBuilder.defaultImpl().start()
                    .put(
                        JSON_KEY_RESOURCE,
                        MapBuilder.defaultImpl().start()
                            .put(JSON_KEY_ODATA_ID, vlmOdataId)
                            .build()
                    )
                    .build()
            );
            if (attachVlmResp.getStatusCode() != HttpHeader.HTTP_NO_CONTENT)
            {
                // problem
                throw new StorageException(
                    String.format(
                        "Unexpected return code from attaching volume %s: %d",
                        attachAction,
                        attachVlmResp.getStatusCode()
                    )
                );
            }
            // volume should be attached.

            // TODO implement health check on composed node

            volumePath = getVolumePath(identifier, cryptKey != null);
        }
        catch (InterruptedException interruptedExc)
        {
            throw new StorageException("poll timeout interrupted", interruptedExc);
        }
        catch (IOException ioExc)
        {
            throw new StorageException("IO Exception", ioExc);
        }
        return volumePath;
    }

    @Override
    public void deleteVolume(String identifier, boolean isEncrypted) throws StorageException
    {
        try
        {
            String vlmOdataId =
                SF_BASE + SF_STORAGE_SERVICES + "/" + storSvc + SF_VOLUMES + "/" +
                getSwordfishVolumeIdByLinstorId(identifier);

            // detach volume from node
            String detachAction = SF_BASE + SF_NODES + "/" + composedNodeName + SF_ACTIONS +
                SF_COMPOSED_NODE_DETACH_RESOURCE;
            // POST to Node/$id/Action/ComposedNode.DetachResource
            RestResponse<Map<String, Object>> detachVlmResp = restClient.execute(
                RestOp.POST,
                hostPort + detachAction,
                getDefaultHeader().build(),
                MapBuilder.defaultImpl().start()
                .put(
                    JSON_KEY_RESOURCE,
                    MapBuilder.defaultImpl().start()
                        .put(JSON_KEY_ODATA_ID, vlmOdataId)
                        .build()
                )
                .build()
            );

            // TODO health check on composed node

            // DELETE to volumes collection
            RestResponse<Map<String, Object>> delVlmResp = restClient.execute(
                RestOp.DELETE,
                hostPort + vlmOdataId,
                getDefaultHeader().noContentType().build(),
                (String) null
            );

            if (delVlmResp.getStatusCode() !=  HttpHeader.HTTP_ACCEPTED)
            {
                throw new StorageException(
                    String.format(
                        "Unexpected return code from DELETE to %s: %d",
                        vlmOdataId,
                        delVlmResp.getStatusCode()
                    )
                );
            }
        }
        catch (IOException ioExc)
        {
            throw new StorageException("IO Exception", ioExc);
        }
        linstorIdToSwordfishId.remove(identifier);
        persistJson();
    }

    @Override
    public boolean volumeExists(String identifier, boolean isEncrypted) throws StorageException
    {
        boolean exists = false;

        // TODO implement encrypted "volumesExists"
        if (getSwordfishVolumeIdByLinstorId(identifier) != null)
        {
            exists = getSwordfishVolumeByLinstorId(identifier).getStatusCode() == HttpHeader.HTTP_OK;
        }
        return exists;
    }

    @Override
    public SizeComparison compareVolumeSize(String identifier, long requiredSize) throws StorageException
    {
        SizeComparison ret;

        RestResponse<Map<String, Object>> response = getSwordfishVolumeByLinstorId(identifier);

        if (response.getStatusCode() == HttpHeader.HTTP_OK)
        {
            // TODO: double check the rest key
            long actualSize = getLong(response.getData().get(JSON_KEY_CAPACITY_BYTES)) / KIB;
            if (actualSize >= requiredSize)
            {
                ret = SizeComparison.WITHIN_TOLERANCE; // no upper bound (yet)... TODO
            }
            else
            {
                ret = SizeComparison.TOO_SMALL;
            }
        }
        else
        {
            throw new StorageException(
                "Could not determine size of swordfish volume: '" + identifier + "'\n" +
                "GET returned status code: " + response.getStatusCode()
            );
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String getVolumePath(String identifier, boolean isEncrypted) throws StorageException
    {
        String path = null;
        if (isEncrypted)
        {
            path = crypt.getCryptVolumePath(identifier);
        }
        else
        {
            try
            {
                String nqnUuid = null;

                RestResponse<Map<String, Object>> vlmInfo = getSwordfishVolumeByLinstorId(identifier);

                Map<String, Object> vlmData = vlmInfo.getData();
                Map<String, Object> vlmLinks = (Map<String, Object>) vlmData.get(JSON_KEY_LINKS);
                Map<String, Object> linksOem = (Map<String, Object>) vlmLinks.get(JSON_KEY_OEM);
                Map<String, Object> oemIntelRackscale = (Map<String, Object>) linksOem.get(JSON_KEY_INTEL_RACK_SCALE);
                ArrayList<Object> intelRackscaleEndpoints = (ArrayList<Object>) oemIntelRackscale.get(JSON_KEY_ENDPOINTS);
                Map<String, Object> endpoint = (Map<String, Object>) intelRackscaleEndpoints.get(0);
                String endpointOdataId = (String) endpoint.get(JSON_KEY_ODATA_ID);

                RestResponse<Map<String, Object>> endpointInfo = getSwordfishResource(endpointOdataId);
                Map<String, Object> endpointData = endpointInfo.getData();
                ArrayList<Object> endpointIdentifiers = (ArrayList<Object>) endpointData.get(JSON_KEY_IDENTIFIERS);
                for (Object endpointIdentifier : endpointIdentifiers)
                {
                    Map<String, Object> endpointIdMap = (Map<String, Object>) endpointIdentifier;
                    if (JSON_VALUE_NQN.equals(endpointIdMap.get(JSON_KEY_DURABLE_NAME_FORMAT)))
                    {
                        String nqn = (String) endpointIdMap.get(JSON_KEY_DURABLE_NAME);
                        nqnUuid = nqn.substring(nqn.lastIndexOf("uuid:") + "uuid:".length());

                        int grepTries = 0;
                        boolean grepFailed = false;
                        boolean grepFound = false;
                        while (!grepFailed && !grepFound)
                        {
                            final ExtCmd extCmd = new ExtCmd(timer, errorReporter);
                            OutputData outputData = extCmd.exec(
                                "/bin/bash",
                                "-c",
                                "grep -H --color=never " + nqnUuid + " /sys/devices/virtual/nvme-fabrics/ctl/nvme*/subsysnqn"
                            );
                            if (outputData.exitCode == 0)
                            {
                                String outString = new String(outputData.stdoutData);
                                Matcher matcher = PATTERN_NQN.matcher(outString);
                                if (matcher.find())
                                {
                                    // although nvme supports multiple namespace, the current implementation
                                    // relies on podmanager's limitation of only supporting one namespace per
                                    // nvme device
                                    path = "/dev/nvme" + matcher.group(1) + "n1";
                                    grepFound = true;
                                }
                            }

                            if (grepTries++ >= 50)
                            {
                                grepFailed = true;
                            }
                            else
                            {
                                Thread.sleep(pollTimeout);
                            }
                        }
                        break;
                    }
                }
                if (path == null)
                {
                    throw new StorageException("Could not extract system path of volume");
                }
            }
            catch (ClassCastException ccExc)
            {
                throw new StorageException("Unexpected json structure", ccExc);
            }
            catch (ChildProcessTimeoutException timeoutExc)
            {
                throw new StorageException("External operation timed out", timeoutExc);
            }
            catch (InterruptedException interruptedExc)
            {
                throw new StorageException("Interrupted exception", interruptedExc);
            }
            catch (IOException ioExc)
            {
                throw new StorageException("IO exception occured", ioExc);
            }

            // TODO: somehow we have to find out the NVME device. for that we need at least
            // the target NQN maybe via the volume?
            // if we have the NQN we can make the following external calls:

            // ext command on initiator from folder
            // /sys/devices/virtual/nvme-fabrics/ctl
            // cmd: grep -H --color=never <target NQN>  nvme*/subsysnqn
            // nvme2/subsysnqn:<target NQN>                 # this is how we get the nvme$number
            // the previous command should also have exitcode 0

//            throw new ImplementationError("Not implemented yet");
        }
        return path;
    }

    @Override
    public long getSize(String identifier) throws StorageException
    {
        return getSpace(
            getSwordfishVolumeByLinstorId(identifier),
            JSON_KEY_ALLOCATED_BYTES
        );
    }

    @Override
    public long getFreeSpace() throws StorageException
    {
        return getSpace(
            getSwordfishPool(),
            JSON_KEY_GUARANTEED_BYTES
        );
    }

    @Override
    public long getTotalSpace() throws StorageException
    {
        return getSpace(
            getSwordfishPool(),
            JSON_KEY_ALLOCATED_BYTES
        );
    }

    @SuppressWarnings("unchecked")
    private long getSpace(RestResponse<Map<String, Object>> restResponse, String key) throws StorageException
    {
        Map<String, Object> poolData = restResponse.getData();
        Long space = null;
        try
        {
            Map<String, Object> capacity = (Map<String, Object>) poolData.get(JSON_KEY_CAPACITY);
            Map<String, Object> capacityData = (Map<String, Object>) capacity.get(JSON_KEY_DATA);
            Object spaceObj = capacityData.get(key);
            if (spaceObj instanceof Integer)
            {
                space = ((Integer)spaceObj).longValue();
            }
            else
            if (spaceObj instanceof Long)
            {
                space = (Long) spaceObj;
            }
        }
        catch (ClassCastException ccExc)
        {
            throw new StorageException("Unexpected json structure in response", ccExc);
        }
        catch (NullPointerException npExc)
        {
            throw new StorageException("Unexpected null entry in response", npExc);
        }
        if (space == null)
        {
            throw new StorageException("Could not retrieve requested space");
        }
        return space;

    }

    @Override
    public Map<String, String> getTraits(String identifier) throws StorageException
    {
        return Collections.emptyMap();
    }

    @Override
    public void setConfiguration(Map<String, String> config) throws StorageException
    {
        // first, check if the config is valid
        boolean requiresHostPort = hostPort == null;
        boolean requiresStorSvc = storSvc == null;
        boolean requiresSfStorPool = storPool == null;
        boolean requiresLsStorPool = linstorIdToSwordfishId == null;
        boolean requiresComposedNodeName = composedNodeName == null;

        String tmpHostPort = getPrefixed(config, StorageConstants.CONFIG_SWORDFISH_HOST_PORT_KEY);
        String tmpStorSvc = getPrefixed(config, StorageConstants.CONFIG_SWORDFISH_STOR_SVC_KEY);
        String tmpSfStorPool = getPrefixed(config, StorageConstants.CONFIG_SWORDFISH_STOR_POOL_KEY);
        String tmpLsStorPool = getPrefixed(config, StorageConstants.CONFIG_SWORDFISH_LINSTOR_STOR_POOL_KEY);
        String tmpUserName = getPrefixed(config, StorageConstants.CONFIG_SWORDFISH_USER_NAME_KEY);
        String tmpUserPw = getPrefixed(config, StorageConstants.CONFIG_SWORDFISH_USER_PW_KEY);
        String tmpTimeout = getPrefixed(config, StorageConstants.CONFIG_SWORDFISH_POLL_TIMEOUT_KEY);
        String tmpComposedNodeName = getPrefixed(config, StorageConstants.CONFIG_SWORDFISH_COMPOSED_NODE_NAME_KEY);
        Long tmpTimeoutLong = null;

        String failErrorMsg = "";
        if ((tmpHostPort == null || tmpHostPort.isEmpty()) && requiresHostPort)
        {
            failErrorMsg += "Missing swordfish host:port specification as a single value such as \n" +
                "https://127.0.0.1:1234\n";
        }
        if ((tmpStorSvc == null || tmpStorSvc.isEmpty()) && requiresStorSvc)
        {
            failErrorMsg += "Missing swordfish storage service\n";
        }
        if ((tmpSfStorPool == null || tmpSfStorPool.isEmpty()) && requiresSfStorPool)
        {
            failErrorMsg += "Missing swordfish storage pool\n";
        }
        if ((tmpLsStorPool == null || tmpLsStorPool.isEmpty()) && requiresLsStorPool)
        {
            failErrorMsg += "Missing linstor storage pool name\n";
        }
        if (tmpTimeout != null && !tmpTimeout.isEmpty())
        {
            try
            {
                tmpTimeoutLong = Long.parseLong(tmpTimeout);
            }
            catch (NumberFormatException nfe)
            {
                failErrorMsg += "Configured timeout value (" + tmpTimeout + ") is not a number";
            }
        }
        if ((tmpComposedNodeName == null || tmpComposedNodeName.isEmpty()) && requiresComposedNodeName)
        {
            failErrorMsg += "Missing swordfish composed node name";
        }

        if (!failErrorMsg.trim().isEmpty())
        {
            throw new StorageException(failErrorMsg);
        }

        // if all was good, apply the values
        if (tmpHostPort != null)
        {
            while (tmpHostPort.endsWith("/"))
            {
                tmpHostPort = tmpHostPort.substring(0, tmpHostPort.length() - 1); // cut the trailing '/'
            }
            if (!tmpHostPort.startsWith("http"))
            {
                tmpHostPort = "http://" + tmpHostPort;
            }
            hostPort = tmpHostPort + "/";
        }
        if (tmpSfStorPool != null)
        {
            storPool = tmpSfStorPool;
        }
        if (tmpLsStorPool != null)
        {
            @SuppressWarnings("unchecked")
            Map<String, String> lut =  (Map<String, String>) JSON_OBJ.get(tmpSfStorPool);
            if (lut == null)
            {
                lut = new HashMap<>();
                JSON_OBJ.put(tmpSfStorPool, lut);
            }
            linstorIdToSwordfishId = lut;
        }
        if (tmpStorSvc != null)
        {
            storSvc = tmpStorSvc;
        }
        if (tmpUserName != null)
        {
            userName = tmpUserName;
        }
        if (tmpUserPw != null)
        {
            userName = tmpUserPw;
        }
        if (tmpTimeoutLong != null)
        {
            pollTimeout = tmpTimeoutLong;
        }
        if (tmpComposedNodeName != null)
        {
            composedNodeName = tmpComposedNodeName;
        }
    }

    private String getPrefixed(Map<String, String> config, String key)
    {
        return config.get(ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + key);
    }

    @Override
    public void resizeVolume(String identifier, long size, String cryptKey)
        throws StorageException, MaxSizeException, MinSizeException
    {
        throw new ImplementationError("Resizing swordfish volumes is not supported");
    }

    @Override
    public void createSnapshot(String identifier, String snapshotName) throws StorageException
    {
        throw new StorageException("Swordfish driver cannot create snapshots");
    }

    @Override
    public void restoreSnapshot(String sourceIdentifier, String snapshotName, String targetIdentifier, String cryptKey)
        throws StorageException
    {
        throw new StorageException("Swordfish driver cannot create or restore snapshots");
    }

    @Override
    public void deleteSnapshot(String identifier, String snapshotName) throws StorageException
    {
        throw new StorageException("Swordfish driver cannot create or delete snapshots");
    }

    @Override
    public boolean snapshotExists(String volumeIdentifier, String snapshotName) throws StorageException
    {
        throw new ImplementationError("Snapshots of swordfish volumes are not supported");
    }

    private RestResponse<Map<String, Object>> getSwordfishPool() throws StorageException
    {
        return getSwordfishResource(
            SF_BASE + SF_STORAGE_SERVICES + "/" + storSvc + SF_STORAGE_POOLS + "/" + storPool
        );
    }

    private RestResponse<Map<String, Object>> getSwordfishVolumeByLinstorId(String identifier) throws StorageException
    {
        return getSwordfishResource(
            SF_BASE + SF_STORAGE_SERVICES + "/" + storSvc + SF_VOLUMES + "/" +
            getSwordfishVolumeIdByLinstorId(identifier)
        );
    }

    private String getSwordfishVolumeIdByLinstorId(String identifier)
    {
        return linstorIdToSwordfishId.get(identifier);
    }

    private RestResponse<Map<String, Object>> getSwordfishResource(String odataId)
        throws StorageException
    {
        RestResponse<Map<String, Object>> rscInfo;
        try
        {
            rscInfo = restClient.execute(
                RestOp.GET,
                hostPort + odataId,
                getDefaultHeader().build(),
                (String) null
            );

            if (rscInfo.getStatusCode() != HttpHeader.HTTP_OK)
            {
                throw new StorageException(
                    "Error receiving info of swordfish resource '" + odataId + "'. Status code: " +
                    rscInfo.getStatusCode()
                );
            }
        }
        catch (IOException ioExc)
        {
            throw new StorageException("IO Exception", ioExc);
        }
        return rscInfo;
    }

    private HttpHeader.Builder getDefaultHeader()
    {
        HttpHeader.Builder httpHeaderBuilder = HttpHeader.newBuilder();
        httpHeaderBuilder.setJsonContentType();
        if (userName != null && !userName.isEmpty())
        {
            httpHeaderBuilder.setAuth(userName, userPw);
        }
        return httpHeaderBuilder;
    }

    private long getLong(Object object)
    {
        long ret;
        if (object instanceof Integer)
        {
            ret = ((Integer) object).longValue();
        }
        else
        {
            ret = (long) object;
        }
        return ret;
    }


    private void persistJson() throws StorageException
    {
        boolean writeComplete = false;
        try
        {
            JSON.std.write(JSON_OBJ, SF_MAPPING_FILE_TMP);
            writeComplete = true;
            Files.move(
                SF_MAPPING_FILE_TMP.toPath(),
                SF_MAPPING_FILE.toPath(),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING
            );
        }
        catch (IOException exc)
        {
            throw new StorageException(
                writeComplete ?
                    "Failed to move swordfish.json.tmp to swordfish.json" :
                    "Failed to write swordfish.json",
                exc
            );
        }
    }
}