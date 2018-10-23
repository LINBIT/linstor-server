package com.linbit.linstor.storage;

import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ALLOCATED_BYTES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_CAPACITY_BYTES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_CAPACITY_SOURCES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_GUARANTEED_BYTES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ODATA_ID;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_PROVIDING_POOLS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.KIB;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_BASE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_MONITOR;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_MAPPING_PATH_FORMAT;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_MAPPING_PATH_TMP_FORMAT;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_STORAGE_POOLS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_STORAGE_SERVICES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_TASKS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_TASK_SERVICE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_VOLUMES;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestClient;
import com.linbit.linstor.storage.utils.RestClient.RestOp;

import com.linbit.linstor.storage.utils.RestResponse;
import com.linbit.linstor.storage.utils.SwordfishConsts;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.jr.ob.JSON;
import com.fasterxml.jackson.jr.ob.impl.CollectionBuilder;
import com.fasterxml.jackson.jr.ob.impl.MapBuilder;

public class SwordfishTargetDriver extends AbsSwordfishDriver
{
    private static final Object SYNC_OBJ = new Object();
    private static Map<String, Object> jsonObj;

    private static final String STATE_CREATING = "Creating";
    private static final String STATE_CREATING_TIMEOUT = "To: Creating";
    private static final String STATE_CREATED = "Created";

    private final StltConfigAccessor stltCfgAccessor;

    private Path jsonPath;
    private Path jsonPathTmp;
    private Map<String, String> linstorIdToSwordfishId;

    private String storSvc;
    private String storPool;
    private long pollVlmCrtTimeout = 600;
    private long pollVlmCrtMaxTries = 100;

    public SwordfishTargetDriver(
        ErrorReporter errorReporterRef,
        SwordfishTargetDriverKind swordfishDriverKindRef,
        RestClient restClientRef,
        StltConfigAccessor stltCfgAccessorRef
    )
    {
        super(errorReporterRef, swordfishDriverKindRef, restClientRef);
        stltCfgAccessor = stltCfgAccessorRef;

        loadJson();
    }


    private void loadJson()
    {
        String nodeName = stltCfgAccessor.getNodeName();
        jsonPath = Paths.get(String.format(SF_MAPPING_PATH_FORMAT, nodeName));
        jsonPathTmp = Paths.get(String.format(SF_MAPPING_PATH_TMP_FORMAT, nodeName));
        synchronized (SYNC_OBJ)
        {
            if (jsonObj == null)
            {
                try
                {
                    String jsonContent = "{}";
                    if (Files.exists(jsonPath))
                    {
                        jsonContent = new String(Files.readAllBytes(jsonPath));
                    }
                    else
                    {
                        Files.createDirectories(jsonPath.getParent());
                        Files.createFile(jsonPath);
                    }
                    if (jsonContent.trim().isEmpty())
                    {
                        jsonObj = JSON.std.mapFrom("{}");
                    }
                    else
                    {
                        jsonObj = JSON.std.mapFrom(jsonContent);
                    }
                    jsonObj = new ConcurrentHashMap<>(jsonObj);
                }
                catch (IOException exc)
                {
                    throw new LinStorRuntimeException("Failed to load swordfish.json", exc);
                }
            }
        }
    }


    @Override
    public void startVolume(String identifier, String cryptKey, Props vlmDfnProps) throws StorageException
    {
    }

    @Override
    public void stopVolume(String identifier, boolean isEncrypted, Props vlmDfnProps) throws StorageException
    {
    }

    @Override
    public String createVolume(
        String linstorVlmId,
        long size,
        String ignoredCryptKey,
        Props vlmDfnProps
    )
        throws StorageException, MaxSizeException, MinSizeException
    {
        try
        {
            String vlmOdataId = null;
            if (!sfVlmExistsByLsVlmId(linstorVlmId))
            {
                vlmOdataId = createSfVlm(linstorVlmId, size);
                // extract the swordfish id of that volume and persist if for later lookups
                String sfId = vlmOdataId.substring(
                    (SF_BASE + SF_STORAGE_SERVICES + "/" + storSvc + SF_VOLUMES + "/").length()
                );
                storeSfVlmId(linstorVlmId, vlmDfnProps, sfId);
                errorReporter.logTrace("volume created with @odata.id: %s",  vlmOdataId);
            }
            else
            {
                vlmOdataId = buildVlmOdataId(linstorVlmId);
                errorReporter.logTrace("volume found with @odata.id: %s", vlmOdataId);
            }
            setState(linstorVlmId, STATE_CREATED);
            // volume exists
        }
        catch (InterruptedException interruptedExc)
        {
            throw new StorageException("poll timeout interrupted", interruptedExc);
        }
        catch (IOException ioExc)
        {
            throw new StorageException("IO Exception", ioExc);
        }
        // swordfishTargetDriver never creates a device, only the remote volume
        return "/dev/null";
    }

    private boolean sfVlmExistsByLsVlmId(String linstorVlmId) throws StorageException
    {
        boolean exists = false;
        String sfVlmId = getSwordfishVolumeIdByLinstorId(linstorVlmId);
        if (sfVlmId != null)
        {
            exists = sfVolumeExists(linstorVlmId, storSvc, sfVlmId);
        }
        return exists;
    }

    private void storeSfVlmId(String linstorVlmId, Props vlmDfnProps, String sfId) throws StorageException
    {
        linstorIdToSwordfishId.put(linstorVlmId, sfId);
        persistJson();
        try
        {
            vlmDfnProps.setProp(SwordfishConsts.DRIVER_SF_VLM_ID_KEY, sfId, StorageDriver.STORAGE_NAMESPACE);
            vlmDfnProps.setProp(SwordfishConsts.DRIVER_SF_STOR_SVC_ID_KEY, storSvc, StorageDriver.STORAGE_NAMESPACE);
        }
        catch (AccessDeniedException | InvalidKeyException | InvalidValueException | SQLException exc)
        {
            throw new StorageException("Cannot persist swordfish volume id into volume definition props", exc);
        }
    }

    private void removeSfVlmId(String linstorVlmId, Props vlmDfnProps) throws StorageException
    {
        linstorIdToSwordfishId.remove(linstorVlmId);
        persistJson();
        try
        {
            vlmDfnProps.removeProp(SwordfishConsts.DRIVER_SF_VLM_ID_KEY, StorageDriver.STORAGE_NAMESPACE);
        }
        catch (AccessDeniedException | InvalidKeyException | SQLException exc)
        {
            throw new StorageException("Cannot persist swordfish volume id into volume definition props", exc);
        }
    }

    private String createSfVlm(String linstorVlmId, long sizeInKiB)
        throws IOException, StorageException, InterruptedException
    {
        // create volume
        // POST to volumes collection
        String volumeCollUrl = sfUrl + SF_BASE + SF_STORAGE_SERVICES + "/" + storSvc + SF_VOLUMES;
        RestResponse<Map<String, Object>> crtVlmResp = restClient.execute(
            linstorVlmId,
            RestOp.POST,
            volumeCollUrl,
            getDefaultHeader().build(),
            MapBuilder.defaultImpl().start()
                .put(JSON_KEY_CAPACITY_BYTES, sizeInKiB * KIB) // convert to bytes
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
                .build(),
            Arrays.asList(HttpHeader.HTTP_ACCEPTED)
        );
        setState(linstorVlmId, STATE_CREATING);
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
        long pollVlmCrtTries = 0;
        while (vlmLocation == null)
        {
            ++pollVlmCrtTries;
            errorReporter.logTrace("waiting %d ms before polling task monitor", pollVlmCrtTimeout);
            Thread.sleep(pollVlmCrtTimeout);

            RestResponse<Map<String, Object>> crtVlmTaskResp = restClient.execute(
                linstorVlmId,
                RestOp.GET,
                sfUrl  + taskMonitorLocation,
                getDefaultHeader().noContentType().build(),
                (String) null,
                Arrays.asList(HttpHeader.HTTP_ACCEPTED, HttpHeader.HTTP_CREATED)
            );
            switch (crtVlmTaskResp.getStatusCode())
            {
                case HttpHeader.HTTP_ACCEPTED: // noop, task is still in progress
                    break;
                case HttpHeader.HTTP_CREATED: // task created successfully
                    vlmLocation = crtVlmTaskResp.getHeaders().get(HttpHeader.LOCATION_KEY);
                    break;
                default: // problem
                    throw new ImplementationError(
                        String.format(
                            "Received none-whitelisted REST status code. URL %s; status code: %d",
                            taskMonitorLocation,
                            crtVlmTaskResp.getStatusCode()
                        )
                    );
            }
            if (pollVlmCrtTries >= pollVlmCrtMaxTries && vlmLocation == null)
            {
                setState(linstorVlmId, STATE_CREATING_TIMEOUT);
                throw new StorageException(
                    String.format(
                        "Volume creation not finished after %d x %dms. \n" +
                        "GET %s did not contain volume-location in http header",
                        pollVlmCrtTries,
                        pollVlmCrtTimeout,
                        sfUrl  + taskMonitorLocation
                    )
                );
            }
        }
        setState(linstorVlmId, STATE_CREATED);
        return vlmLocation;
    }

    @Override
    public void deleteVolume(String linstorVlmId, boolean isEncrypted, Props vlmDfnProps) throws StorageException
    {
        try
        {
            String sfVlmId = getSwordfishVolumeIdByLinstorId(linstorVlmId);
            if (sfVlmId == null)
            {
                /*
                 *  two cases:
                 *  1)  we failed to create the volume and now cleaning up
                 *  -> no-op
                 *  2) persisted json does no longer have the the sworfish-id.
                 *  -> check if volumeDefinitionProps has it.
                 *  ---> if yes: impl error
                 *  ---> if no: most likely case 1). Even if not, there is nothing we can do at this point
                 */
                sfVlmId = getSfVlmId(vlmDfnProps, true);
                if (sfVlmId != null)
                {
                    throw new ImplementationError(
                        String.format(
                            "Persisted JSON does not contain sworfish-volume id for linstor volume '%s'.%n" +
                            "The volume definition properties however has the entry of '%s'.",
                            linstorVlmId,
                            sfVlmId
                        )
                    );
                }
            }
            else
            {
                String vlmOdataId = buildVlmOdataId(storSvc, sfVlmId);

                // TODO health check on composed node

                // DELETE to volumes collection
                restClient.execute(
                    linstorVlmId,
                    RestOp.DELETE,
                    sfUrl + vlmOdataId,
                    getDefaultHeader().noContentType().build(),
                    (String) null,
                    Arrays.asList(HttpHeader.HTTP_ACCEPTED, HttpHeader.HTTP_NOT_FOUND)
                );
                removeSfVlmId(linstorVlmId, vlmDfnProps);
            }
            removeState(linstorVlmId); // deleting
        }
        catch (IOException ioExc)
        {
            throw new StorageException("IO Exception", ioExc);
        }
    }

    @Override
    public boolean volumeExists(String linstorVlmId, boolean isEncrypted, Props vlmDfnProps) throws StorageException
    {
        boolean exists = false;

        String sfVlmId = getSwordfishVolumeIdByLinstorId(linstorVlmId);
        if (sfVlmId != null)
        {
            exists = sfVolumeExists(linstorVlmId, storSvc, sfVlmId);
        }
        if (exists)
        {
            setState(linstorVlmId, STATE_CREATED);
        }
        return exists;
    }

    @Override
    public SizeComparison compareVolumeSize(String linstorVlmId, long requiredSize, Props vlmDfnProps)
        throws StorageException
    {
        return compareVolumeSizeImpl(
            linstorVlmId,
            storSvc,
            getSwordfishVolumeIdByLinstorId(linstorVlmId),
            requiredSize
        );
    }

    @Override
    public String getVolumePath(String linstorVlmId, boolean isEncrypted, Props vlmDfnProps) throws StorageException
    {
        // swordfishTARGETDriver never creates a usable device (thus, no volume path)
        // only an attachable remote volume
        return "/dev/null";
    }

    @Override
    public long getSize(String linstorVlmId, Props vlmDfnProps) throws StorageException
    {
        return getSpace(
            getSwordfishVolumeByLinstorId(linstorVlmId),
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

    @Override
    public Map<String, String> getTraits(String linstorVlmId) throws StorageException
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
        boolean requiresStorSvc = storSvc == null;
        boolean requiresSfStorPool = storPool == null;

        String tmpStorSvc = nodeNamespace.get(StorageConstants.CONFIG_SF_STOR_SVC_KEY);
        String tmpSfStorPool = storPoolNamespace.get(StorageConstants.CONFIG_SF_STOR_POOL_KEY);
        String tmpVlmCrtTimeout = storPoolNamespace.get(StorageConstants.CONFIG_SF_POLL_TIMEOUT_VLM_CRT_KEY);
        String tmpVlmCrtRetries = storPoolNamespace.get(StorageConstants.CONFIG_SF_POLL_RETRIES_VLM_CRT_KEY);

        StringBuilder failErrorMsg = new StringBuilder();

        appendIfEmptyButRequired("Missing swordfish storage service\n", failErrorMsg, tmpStorSvc, requiresStorSvc);
        appendIfEmptyButRequired("Missing swordfish storage pool\n", failErrorMsg, tmpSfStorPool, requiresSfStorPool);
        Long tmpVlmCrtTimeoutLong = getLong("poll volume creation timeout", failErrorMsg, tmpVlmCrtTimeout);
        Long tmpVlmCrtTriesLong = getLong("poll volume creation tries", failErrorMsg, tmpVlmCrtRetries);

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
        if (tmpSfStorPool != null)
        {
            storPool = tmpSfStorPool;
        }
        if (tmpStorSvc != null)
        {
            storSvc = tmpStorSvc;
        }
        if (tmpVlmCrtTimeoutLong != null)
        {
            pollVlmCrtTimeout = tmpVlmCrtTimeoutLong;
        }
        if (tmpVlmCrtTriesLong != null)
        {
            pollVlmCrtMaxTries = tmpVlmCrtTriesLong;
        }

        @SuppressWarnings("unchecked")
        Map<String, String> lut =  (Map<String, String>) jsonObj.get(storPoolNameStr);
        if (lut == null)
        {
            lut = new HashMap<>();
            jsonObj.put(storPoolNameStr, lut);
        }
        linstorIdToSwordfishId = lut;
    }

    @Override
    public void resizeVolume(String linstorVlmId, long size, String cryptKey, Props vlmDfnProps)
        throws StorageException, MaxSizeException, MinSizeException
    {
        throw new ImplementationError("Resizing swordfish volumes is not supported");
    }

    private RestResponse<Map<String, Object>> getSwordfishPool() throws StorageException
    {
        return getSwordfishResource(
            "",
            SF_BASE + SF_STORAGE_SERVICES + "/" + storSvc + SF_STORAGE_POOLS + "/" + storPool,
            false
        );
    }

    private RestResponse<Map<String, Object>> getSwordfishVolumeByLinstorId(String linstorVlmId) throws StorageException
    {
        return getSwordfishResource(linstorVlmId, buildVlmOdataId(linstorVlmId), false);
    }

    private String buildVlmOdataId(String linstorVlmId)
    {
        return buildVlmOdataId(storSvc, getSwordfishVolumeIdByLinstorId(linstorVlmId));
    }

    private String getSwordfishVolumeIdByLinstorId(String linstorVlmId)
    {
        return linstorIdToSwordfishId.get(linstorVlmId);
    }

    private void persistJson() throws StorageException
    {
        synchronized (SYNC_OBJ)
        {
            boolean writeComplete = false;
            try
            {
                JSON.std.write(jsonObj, jsonPathTmp.toFile());
                writeComplete = true;
                Files.move(
                    jsonPathTmp,
                    jsonPath,
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
}
