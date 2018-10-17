package com.linbit.linstor.storage;

import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ALLOCATED_BYTES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ALLOWABLE_VALUES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_CONNECTED_ENTITIES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_DURABLE_NAME;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_DURABLE_NAME_FORMAT;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ENDPOINTS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ENTITY_ROLE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_IDENTIFIERS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_INTEL_RACK_SCALE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_LINKS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ODATA_ID;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_OEM;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_PARAMETERS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_RESOURCE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_VALUE_NQN;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_VALUE_TARGET;
import static com.linbit.linstor.storage.utils.SwordfishConsts.PATTERN_NQN;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_ACTIONS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_ATTACH_RESOURCE_ACTION_INFO;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_BASE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_COMPOSED_NODE_ATTACH_RESOURCE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_COMPOSED_NODE_DETACH_RESOURCE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_NODES;
import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.utils.Crypt;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestClient;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import com.linbit.linstor.storage.utils.RestResponse;
import com.linbit.linstor.storage.utils.SwordfishConsts;
import com.linbit.linstor.timer.CoreTimer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import com.fasterxml.jackson.jr.ob.impl.MapBuilder;

public class SwordfishInitiatorDriver extends AbsSwordfishDriver
{
    private static final String STATE_WAITING_ATTACHABLE = "Wf: Attachable";
    private static final String STATE_WAITING_ATTACHABLE_TIMEOUT = "To: Attachable";
    private static final String STATE_ATTACHABLE = "Attachable";
    private static final String STATE_ATTACHING = "Attaching";
    private static final String STATE_ATTACHING_TIMEOUT = "To: Attaching";
    private static final String STATE_ATTACHED = "Attached";
    private static final String STATE_DETACHING = "Detaching";

    private long pollAttachVlmTimeout = 1000;
    private long pollAttachVlmMaxTries = 300;
    private long pollGrepNvmeUuidTimeout = 1000;
    private long pollGrepNvmeUuidMaxTries = 300;
    private String composedNodeName;

    private final Crypt crypt;
    private final CoreTimer timer;

    public SwordfishInitiatorDriver(
        ErrorReporter errorReporterRef,
        SwordfishInitiatorDriverKind swordfishDriverKindRef,
        RestClient restClientRef,
        CoreTimer timerRef,
        Crypt cryptRef
    )
    {
        super(errorReporterRef, swordfishDriverKindRef, restClientRef);
        timer = timerRef;
        crypt = cryptRef;
    }


    @Override
    public void startVolume(String identifier, String cryptKey, Props vlmDfnProps) throws StorageException
    {
        if (cryptKey != null)
        {
            crypt.openCryptDevice(
                getVolumePath(identifier, false, vlmDfnProps),
                identifier,
                cryptKey.getBytes()
            );
        }
    }

    @Override
    public void stopVolume(String identifier, boolean isEncrypted, Props vlmDfnProps) throws StorageException
    {
        if (isEncrypted)
        {
            crypt.closeCryptDevice(identifier);
        }
    }

    @Override
    public String createVolume(
        String linstorVlmId,
        long ignoredSize,
        String cryptKey,
        Props vlmDfnProps
    )
        throws StorageException, MaxSizeException, MinSizeException
    {
        String volumePath;
        try
        {
            String sfStorSvcId = getSfStorSvcId(vlmDfnProps);
            String sfVlmId = getSfVlmId(vlmDfnProps, false);
            String vlmOdataId = buildVlmOdataId(sfStorSvcId, sfVlmId);

            if (!isSfVolumeAttached(linstorVlmId, vlmDfnProps))
            {
                waitUntilSfVlmIsAttachable(linstorVlmId, vlmOdataId);
                attachSfVolume(linstorVlmId, vlmOdataId);
            }
            else
            {
                setState(linstorVlmId, STATE_ATTACHABLE);
            }

            // TODO implement health check on composed node

            volumePath = getVolumePath(linstorVlmId, cryptKey != null, vlmDfnProps);
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

    private void attachSfVolume(String linstorVlmId, String vlmOdataId) throws IOException, StorageException
    {
        String attachAction = SF_BASE + SF_NODES + "/" + composedNodeName + SF_ACTIONS +
            SF_COMPOSED_NODE_ATTACH_RESOURCE;
        restClient.execute(
            linstorVlmId,
            RestOp.POST,
            sfUrl + attachAction,
            getDefaultHeader().build(),
            MapBuilder.defaultImpl().start()
                .put(
                    JSON_KEY_RESOURCE,
                    MapBuilder.defaultImpl().start()
                        .put(JSON_KEY_ODATA_ID, vlmOdataId)
                        .build()
                )
                .build(),
            Arrays.asList(HttpHeader.HTTP_NO_CONTENT)
        );
        setState(linstorVlmId, STATE_ATTACHING);
    }

    @SuppressWarnings("unchecked")
    private void waitUntilSfVlmIsAttachable(String linstorVlmId, String vlmOdataId)
        throws InterruptedException, IOException, StorageException
    {
        String attachInfoAction = SF_BASE + SF_NODES + "/" + composedNodeName + SF_ACTIONS +
            SF_ATTACH_RESOURCE_ACTION_INFO;
        boolean attachable = false;

        setState(linstorVlmId, STATE_WAITING_ATTACHABLE);

        int pollAttachRscTries = 0;
        while (!attachable)
        {
            errorReporter.logTrace("waiting %d ms before polling node's Actions/AttachResourceActionInfo",
                                   pollAttachVlmTimeout);
            Thread.sleep(pollAttachVlmTimeout);

            RestResponse<Map<String, Object>> attachRscInfoResp = restClient.execute(
                linstorVlmId,
                RestOp.GET,
                sfUrl  + attachInfoAction,
                getDefaultHeader().noContentType().build(),
                (String) null,
                Arrays.asList(HttpHeader.HTTP_OK)
            );
            Map<String, Object> attachRscInfoData = attachRscInfoResp.getData();
            ArrayList<Object> attachInfoParameters = (ArrayList<Object>) attachRscInfoData.get(JSON_KEY_PARAMETERS);
            for (Object attachInfoParameter : attachInfoParameters)
            {
                Map<String, Object> attachInfoParamMap = (Map<String, Object>) attachInfoParameter;
                ArrayList<Object> paramAllowableValues =
                    (ArrayList<Object>) attachInfoParamMap.get(JSON_KEY_ALLOWABLE_VALUES);
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

            if (++pollAttachRscTries >= pollAttachVlmMaxTries)
            {
                setState(linstorVlmId, STATE_WAITING_ATTACHABLE_TIMEOUT);
                throw new StorageException(
                    String.format(
                        "Volume could not be attached after %d x %dms. \n" +
                        "Volume did not show up in %s -> %s from GET %s",
                        pollAttachRscTries,
                        pollAttachVlmTimeout,
                        JSON_KEY_PARAMETERS,
                        JSON_KEY_ALLOWABLE_VALUES,
                        sfUrl  + attachInfoAction
                    )
                );
            }
        }
        setState(linstorVlmId, STATE_ATTACHABLE);
    }

    @Override
    public void deleteVolume(String linstorVlmId, boolean isEncrypted, Props vlmDfnProps) throws StorageException
    {
        try
        {
            detatchSfVlm(linstorVlmId, vlmDfnProps);

            // TODO health check on composed node
        }
        catch (IOException ioExc)
        {
            throw new StorageException("IO Exception", ioExc);
        }
    }

    @SuppressWarnings("unchecked")
    private String detatchSfVlm(String linstorVlmId, Props vlmDfnProps) throws IOException, StorageException
    {
        String sfStorSvcId = getSfStorSvcId(vlmDfnProps);
        String sfVlmId = getSfVlmId(vlmDfnProps, true);
        String vlmOdataId = null;

        if (sfVlmId != null)
        {
            vlmOdataId = buildVlmOdataId(sfStorSvcId, sfVlmId);
            // detach volume from node
            String detachAction = SF_BASE + SF_NODES + "/" + composedNodeName + SF_ACTIONS +
                SF_COMPOSED_NODE_DETACH_RESOURCE;
            // POST to Node/$id/Action/ComposedNode.DetachResource
            RestResponse<Map<String, Object>> detachVlmResp = restClient.execute(
                linstorVlmId,
                RestOp.POST,
                sfUrl + detachAction,
                getDefaultHeader().build(),
                MapBuilder.defaultImpl().start()
                    .put(
                        JSON_KEY_RESOURCE,
                        MapBuilder.defaultImpl().start()
                            .put(JSON_KEY_ODATA_ID, vlmOdataId)
                            .build()
                    )
                    .build(),
                Arrays.asList(HttpHeader.HTTP_NO_CONTENT, HttpHeader.HTTP_NOT_FOUND, HttpHeader.HTTP_BAD_REQUEST)
            );
            setState(linstorVlmId, STATE_DETACHING);

            switch (detachVlmResp.getStatusCode())
            {
                case HttpHeader.HTTP_BAD_REQUEST:
                    Map<String, Object> errorMap = (Map<String, Object>) detachVlmResp.getData().get("error");
                    List<Map<String, Object>> extInfo = (List<Map<String, Object>>) errorMap
                        .get("@Message.ExtendedInfo");
                    boolean found = false;
                    for (Map<String, Object> sfErrMsgEntry : extInfo)
                    {
                        String errMsg = (String) sfErrMsgEntry.get("Message");
                        if (errMsg.contains("not attached"))
                        {
                            errorReporter.logWarning(
                                String.format(
                                    "Bad request to detach swordfish volume '%s' (not attached).",
                                    sfVlmId
                                )
                            );
                            found = true;
                        }
                    }
                    if (!found)
                    {
                        throw new StorageException(
                            "Unexpected status code",
                            "A REST call returned the unexpected status code " + detachVlmResp.getStatusCode(),
                            null,
                            null,
                            detachVlmResp.toString()
                        );
                    }
                    break;
                case HttpHeader.HTTP_NO_CONTENT:
                case HttpHeader.HTTP_NOT_FOUND:
                default:
                    break;
            }

            removeState(linstorVlmId); // deleting
        }
        else
        {
            errorReporter.logError("Swordfish volume id is null");
        }
        return vlmOdataId;
    }

    @Override
    public boolean volumeExists(String linstorVlmId, boolean isEncrypted, Props vlmDfnProps) throws StorageException
    {
        boolean exists = false;

        // TODO implement encrypted "volumesExists"
        String sfStorSvcId = getSfStorSvcId(vlmDfnProps);
        String sfVlmId = null;
        try
        {
            sfVlmId = getSfVlmId(vlmDfnProps, false);
            exists = sfVolumeExists(linstorVlmId, sfStorSvcId, sfVlmId) &&
                isSfVolumeAttached(linstorVlmId, vlmDfnProps) &&
                getVolumePath(linstorVlmId, isEncrypted, vlmDfnProps) != null;
            if (exists)
            {
                setState(linstorVlmId, STATE_ATTACHED);
            }
        }
        catch (StorageException storExc)
        {
            if (sfVlmId == null)
            {
                errorReporter.logError("Swordfish volume id property not set, assume volume does not exist.");
            }
            else
            {
                throw storExc;
            }
        }
        return exists;
    }

    private String getSfVlmId(Props vlmDfnProps, boolean allowNull) throws StorageException
    {
        return getSfProp(
            vlmDfnProps,
            SwordfishConsts.DRIVER_SF_VLM_ID_KEY,
            "volume id",
            allowNull
        );
    }

    private String getSfStorSvcId(Props vlmDfnProps) throws StorageException
    {
        return getSfProp(
            vlmDfnProps,
            SwordfishConsts.DRIVER_SF_STOR_SVC_ID_KEY,
            "storage service id",
            false
        );
    }

    private String getSfProp(Props vlmDfnProps, String propKey, String errObjDescr, boolean allowNull)
        throws StorageException, ImplementationError
    {
        String sfVlmId;
        try
        {
            sfVlmId = vlmDfnProps.getProp(
                propKey,
                StorageDriver.STORAGE_NAMESPACE
            );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        if (!allowNull && sfVlmId == null)
        {
            throw new StorageException(
                "No swordfish " + errObjDescr + " given. This usually happens if you forgot to " +
                "create a resource of this resource definition using a swordfishTargetDriver"
            );
        }
        return sfVlmId;
    }

    private boolean isSfVolumeAttached(String linstorVlmId, Props vlmDfnProps)
        throws StorageException
    {
        return getSfVolumeEndpointDurableNameNqn(linstorVlmId, vlmDfnProps) != null;
    }

    @Override
    public SizeComparison compareVolumeSize(String linstorVlmId, long requiredSize, Props vlmDfnProps)
        throws StorageException
    {
        return compareVolumeSizeImpl(
            linstorVlmId,
            getSfStorSvcId(vlmDfnProps),
            getSfVlmId(vlmDfnProps, false),
            requiredSize
        );
    }

    @Override
    public String getVolumePath(String linstorVlmId, boolean isEncrypted, Props vlmDfnProps) throws StorageException
    {
        String path = null;
        if (isEncrypted)
        {
            path = crypt.getCryptVolumePath(linstorVlmId);
        }
        else
        {
            try
            {
                String nqnUuid = null;

                String nqn = getSfVolumeEndpointDurableNameNqn(linstorVlmId, vlmDfnProps);

                if (nqn != null)
                {
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
                            "grep -H --color=never " + nqnUuid +
                            " /sys/devices/virtual/nvme-fabrics/ctl/nvme*/subsysnqn"
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
                                setState(linstorVlmId, STATE_ATTACHED);
                            }
                        }

                        if (++grepTries >= pollGrepNvmeUuidMaxTries)
                        {
                            setState(linstorVlmId, STATE_ATTACHING_TIMEOUT);
                            grepFailed = true;
                        }
                        else
                        {
                            Thread.sleep(pollGrepNvmeUuidTimeout);
                        }
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
        }
        return path;
    }

    @SuppressWarnings("unchecked")
    private String getSfVolumeEndpointDurableNameNqn(String linstorVlmId, Props vlmDfnProps) throws StorageException
    {
        String sfStorSvcId = getSfStorSvcId(vlmDfnProps);
        String sfVlmId = getSfVlmId(vlmDfnProps, false);
        RestResponse<Map<String, Object>> vlmInfo = getSfVlm(linstorVlmId, sfStorSvcId, sfVlmId);

        Map<String, Object> vlmData = vlmInfo.getData();
        Map<String, Object> vlmLinks = (Map<String, Object>) vlmData.get(JSON_KEY_LINKS);
        Map<String, Object> linksOem = (Map<String, Object>) vlmLinks.get(JSON_KEY_OEM);
        Map<String, Object> oemIntelRackscale = (Map<String, Object>) linksOem.get(JSON_KEY_INTEL_RACK_SCALE);
        ArrayList<Object> intelRackscaleEndpoints =
            (ArrayList<Object>) oemIntelRackscale.get(JSON_KEY_ENDPOINTS);

        String nqn = null;
        for (Object endpointObj : intelRackscaleEndpoints)
        {
            Map<String, Object> endpoint = (Map<String, Object>) endpointObj;

            String endpointOdataId = (String) endpoint.get(JSON_KEY_ODATA_ID);

            RestResponse<Map<String, Object>> endpointInfo = getSwordfishResource(linstorVlmId, endpointOdataId);
            Map<String, Object> endpointData = endpointInfo.getData();

            List<Map<String, Object>> connectedEntities =
                (List<Map<String, Object>>) endpointData.get(JSON_KEY_CONNECTED_ENTITIES);
            for (Map<String, Object> connectedEntity : connectedEntities)
            {
                if (connectedEntity.get(JSON_KEY_ENTITY_ROLE).equals(JSON_VALUE_TARGET))
                {
                    List<Object> endpointIdentifiers =
                        (List<Object>) endpointData.get(JSON_KEY_IDENTIFIERS);
                    for (Object endpointIdentifier : endpointIdentifiers)
                    {
                        Map<String, Object> endpointIdMap = (Map<String, Object>) endpointIdentifier;

                        if (JSON_VALUE_NQN.equals(endpointIdMap.get(JSON_KEY_DURABLE_NAME_FORMAT)))
                        {
                            nqn = (String) endpointIdMap.get(JSON_KEY_DURABLE_NAME);
                            break;
                        }
                    }
                    if (nqn != null)
                    {
                        break;
                    }
                }
            }
        }
        return nqn;
    }

    @Override
    public long getSize(String linstorVlmId, Props vlmDfnProps) throws StorageException
    {
        return getSpace(
            getSfVlm(linstorVlmId, getSfStorSvcId(vlmDfnProps), getSfVlmId(vlmDfnProps, false)),
            JSON_KEY_ALLOCATED_BYTES
        );
    }

    @Override
    public long getFreeSpace() throws StorageException
    {
        return Long.MAX_VALUE;
        // return getSpace(
        //    getSwordfishPool(),
        //    JSON_KEY_GUARANTEED_BYTES
        // );
    }

    @Override
    public long getTotalSpace() throws StorageException
    {
        return Long.MAX_VALUE;
        // return getSpace(
        //    getSwordfishPool(),
        //    JSON_KEY_ALLOCATED_BYTES
        // );
    }

    @Override
    public Map<String, String> getTraits(String ignore) throws StorageException
    {
        return Collections.emptyMap();
    }

    @Override
    public void setConfiguration(
        String storPoolNameStr,
        Map<String, String> storPoolNamespace,
        Map<String, String> nodeNamespace,
        Map<String, String> stltNamespace
    )
        throws StorageException
    {
        // first, check if the config is valid
        boolean requiresComposedNodeName = composedNodeName == null;

        String tmpComposedNodeName = nodeNamespace.get(StorageConstants.CONFIG_SF_COMPOSED_NODE_NAME_KEY);
        String tmpAttachVlmTimeout = storPoolNamespace.get(StorageConstants.CONFIG_SF_POLL_TIMEOUT_ATTACH_VLM_KEY);
        String tmpAttachVlmRetries = storPoolNamespace.get(StorageConstants.CONFIG_SF_POLL_RETRIES_ATTACH_VLM_KEY);
        String tmpGrepNvmeUuidTimeout =
            storPoolNamespace.get(StorageConstants.CONFIG_SF_POLL_TIMEOUT_GREP_NVME_UUID_KEY);
        String tmpGrepNvmeUuidRetries =
            storPoolNamespace.get(StorageConstants.CONFIG_SF_POLL_RETRIES_GREP_NVME_UUID_KEY);

        StringBuilder failErrorMsg = new StringBuilder();
        appendIfEmptyButRequired(
            "Missing swordfish composed node name\n" +
                "This property has to be set on node level: \n\n" +
                "linstor node set-property <node_name> " +
                ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + StorageConstants.CONFIG_SF_COMPOSED_NODE_NAME_KEY +
                " <composed_node_id>",
            failErrorMsg,
            tmpComposedNodeName,
            requiresComposedNodeName
        );
        Long tmpAttachVlmTimeoutLong = getLong("poll attach volume timeout", failErrorMsg, tmpAttachVlmTimeout);
        Long tmpAttachVlmTriesLong = getLong("poll attach volume tries", failErrorMsg, tmpAttachVlmRetries);
        Long tmpGrepNvmeUuidTimeoutLong = getLong("poll grep nvme uuid timeout", failErrorMsg, tmpGrepNvmeUuidTimeout);
        Long tmpGrepNvmeUuidTriesLong = getLong("poll grep nvme uuid tries", failErrorMsg, tmpGrepNvmeUuidRetries);

        //TODO: perform a check if this compute node is really the confugired composed node
        // i.e. `uname -n` maches /redfish/v1/Nodes/<composedNodeId> -> "Name"

        if (!failErrorMsg.toString().trim().isEmpty())
        {
            throw new StorageException(failErrorMsg.toString());
        }

        // set sf-url, username and userpw
        super.setConfiguration(
            storPoolNameStr,
            storPoolNamespace,
            nodeNamespace,
            stltNamespace
        );

        // if all was good, apply the values
        if (tmpAttachVlmTimeoutLong != null)
        {
            pollAttachVlmTimeout = tmpAttachVlmTimeoutLong;
        }
        if (tmpAttachVlmTriesLong != null)
        {
            pollAttachVlmMaxTries = tmpAttachVlmTriesLong;
        }
        if (tmpGrepNvmeUuidTimeoutLong != null)
        {
            pollGrepNvmeUuidTimeout = tmpGrepNvmeUuidTimeoutLong;
        }
        if (tmpGrepNvmeUuidTriesLong != null)
        {
            pollGrepNvmeUuidMaxTries = tmpGrepNvmeUuidTriesLong;
        }
        if (tmpComposedNodeName != null)
        {
            composedNodeName = tmpComposedNodeName;
        }
    }

    @Override
    public void resizeVolume(String ignore, long size, String cryptKey, Props vlmDfnProps)
        throws StorageException, MaxSizeException, MinSizeException
    {
        throw new ImplementationError("Resizing swordfish volumes is not supported");
    }

    private RestResponse<Map<String, Object>> getSfVlm(String linstorVlmId, String sfStorSvcId, String sfVlmId)
        throws StorageException
    {
        return getSwordfishResource(linstorVlmId, buildVlmOdataId(sfStorSvcId, sfVlmId));
    }
}
