package com.linbit.linstor.layer.drbd;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.extproc.ChildProcessHandler.TimeoutType;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.ReadOnlyOpener;
import com.linbit.linstor.layer.ReadOnlySnapshotHandle;
import com.linbit.linstor.layer.drbd.utils.DrbdAdm;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Keeps the snapshots read by CBT sessions open (the layers below DRBD, read-only) and dumps the extents of their
 * volumes from their DRBD metadata: changed since the previous CBT snapshot (dirty) and never written (zero). The
 * controller opens a session's snapshot when the session starts and closes it when the session finishes. An extents
 * request reopens it if this satellite lost its handles in between (restart, reconnect to the controller).
 */
@Singleton
public class CbtSessionService
{
    /** drbdmeta cbt-dump exits with this code if the metadata holds nothing trustworthy for the mode. */
    static final int CBT_DUMP_NO_TRACKING_RC = 50;
    /** the "format" of drbdmeta cbt-dump's output */
    static final int CBT_DUMP_FORMAT = 1;

    private static final String OPENER_SUFFIX = "_cbt";
    /** the dump scans the bitmap up to the end of the volume if it finds few extents */
    private static final long DUMP_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10);
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
    /** length of the drbdmeta output quoted in an error */
    private static final int ABBREVIATE_LEN = 200;

    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;
    private final Provider<DeviceHandler> deviceHandlerProvider;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ReadWriteLock reconfigurationLock;

    private final Map<SessionKey, Session> sessions = new ConcurrentHashMap<>();

    @Inject
    public CbtSessionService(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler> deviceHandlerProviderRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef
    )
    {
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        deviceHandlerProvider = deviceHandlerProviderRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        rscDfnMap = rscDfnMapRef;
        reconfigurationLock = reconfigurationLockRef;
    }

    /**
     * Opens the snapshot of the session, if it is not open yet.
     */
    public void open(ResourceName rscNameRef, SnapshotName snapNameRef)
        throws StorageException, DatabaseException
    {
        getOrOpen(new SessionKey(rscNameRef, snapNameRef));
    }

    /**
     * Closes the snapshot of the session, if it is open. Waits for a running dump of the session.
     */
    public void close(ResourceName rscNameRef, SnapshotName snapNameRef) throws StorageException
    {
        @Nullable Session session = sessions.get(new SessionKey(rscNameRef, snapNameRef));
        if (session != null)
        {
            synchronized (session)
            {
                Lock writeLock = reconfigurationLock.writeLock();
                writeLock.lock();
                try
                {
                    sessions.remove(session.key, session);
                    session.close();
                }
                finally
                {
                    writeLock.unlock();
                }
            }
        }
    }

    /**
     * Closes all sessions, e.g. because the core objects they refer to are about to be replaced. Expects the
     * reconfiguration write lock to be held.
     */
    public void clear()
    {
        for (Session session : new ArrayList<>(sessions.values()))
        {
            sessions.remove(session.key, session);
            try
            {
                session.close();
            }
            catch (StorageException exc)
            {
                errorReporter.reportError(exc);
            }
        }
    }

    /**
     * The error entries a failed session request answers with: an {@link ApiRcException}'s own entries, an error
     * report and {@link ApiConsts#FAIL_UNKNOWN_ERROR} for anything else.
     */
    public ApiCallRc toApiCallRc(Exception excRef)
    {
        ApiCallRc ret;
        if (excRef instanceof ApiRcException apiRcExc)
        {
            ret = apiRcExc.getApiCallRc();
        }
        else
        {
            String reportId = errorReporter.reportError(excRef);
            ApiCallRcImpl.EntryBuilder builder = ApiCallRcImpl.entryBuilder(
                ApiConsts.FAIL_UNKNOWN_ERROR,
                "CBT session request failed: " + excRef.getMessage()
            )
                .addErrorId(reportId);
            if (excRef instanceof LinStorException linExc)
            {
                builder.setCause(linExc.getCauseText()).setDetails(linExc.getDetailsText());
            }
            ret = ApiCallRcImpl.singletonApiCallRc(builder.build());
        }
        return ret;
    }

    public Set<SessionKey> getOpenSessions()
    {
        return Set.copyOf(sessions.keySet());
    }

    /**
     * Dumps up to maxExtentsRef extents of the volume from startOffsetRef (bytes) on. The extents cover the volume
     * without gaps. Requests of the same session are serialized.
     *
     * @throws ApiRcException with {@link ApiConsts#FAIL_CBT_FULL_BACKUP_REQUIRED} if the snapshot holds nothing
     *     trustworthy for the mode: no tracking interval (dirty), or no record of the never written ranges (zero)
     */
    public CbtExtents dump(
        ResourceName rscNameRef,
        SnapshotName snapNameRef,
        int vlmNrRef,
        long startOffsetRef,
        int maxExtentsRef,
        ExtentsMode modeRef
    )
        throws StorageException, DatabaseException
    {
        if (maxExtentsRef < 1 || maxExtentsRef > InternalApiConsts.CBT_MAX_EXTENTS_LIMIT || startOffsetRef < 0)
        {
            throw new ImplementationError(
                "Invalid extents request: start offset " + startOffsetRef + ", max extents " + maxExtentsRef
            );
        }
        Session session = getOrOpen(new SessionKey(rscNameRef, snapNameRef));
        synchronized (session)
        {
            if (session.closed)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_IN_USE,
                        "The CBT session of snapshot '" + snapNameRef.displayValue + "' of resource '" +
                            rscNameRef.displayValue + "' was closed during the request"
                    )
                );
            }
            @Nullable MetaDataDevice mdDev = session.metaDataDevices.get(vlmNrRef);
            if (mdDev == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_VLM_DFN,
                        "Snapshot '" + snapNameRef.displayValue + "' of resource '" + rscNameRef.displayValue +
                            "' has no volume " + vlmNrRef
                    )
                );
            }
            return runDump(mdDev, startOffsetRef, maxExtentsRef, modeRef);
        }
    }

    private Session getOrOpen(SessionKey keyRef) throws StorageException, DatabaseException
    {
        @Nullable Session session = sessions.get(keyRef);
        if (session == null)
        {
            Lock writeLock = reconfigurationLock.writeLock();
            writeLock.lock();
            try
            {
                session = sessions.get(keyRef);
                if (session == null)
                {
                    session = openSession(keyRef);
                    sessions.put(keyRef, session);
                }
            }
            finally
            {
                writeLock.unlock();
            }
        }
        return session;
    }

    private Session openSession(SessionKey keyRef) throws StorageException, DatabaseException
    {
        Snapshot snap = getSnapshot(keyRef);
        Set<AbsRscLayerObject<Snapshot>> drbdSnapDataSet = LayerRscUtils.getRscDataByLayer(
            snap.getLayerData(),
            DeviceLayerKind.DRBD,
            RscLayerSuffixes.SUFFIX_DATA::equals
        );
        if (drbdSnapDataSet.size() != 1)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_CBT_NOT_SUPPORTED,
                    "Snapshot '" + keyRef.snapName.displayValue + "' of resource '" + keyRef.rscName.displayValue +
                        "' has no DRBD layer"
                )
            );
        }

        Session session = new Session(keyRef);
        try
        {
            for (VlmProviderObject<Snapshot> vlmData : drbdSnapDataSet.iterator().next().getVlmLayerObjects().values())
            {
                DrbdVlmData<Snapshot> drbdSnapVlm = (DrbdVlmData<Snapshot>) vlmData;
                // only the device holding the metadata is read, with external metadata the data stays closed
                MetaDataDevice mdDev;
                if (drbdSnapVlm.isUsingExternalMetaData())
                {
                    mdDev = new MetaDataDevice(
                        openChild(session, drbdSnapVlm, RscLayerSuffixes.SUFFIX_DRBD_META),
                        "flex-external"
                    );
                }
                else
                {
                    mdDev = new MetaDataDevice(
                        openChild(session, drbdSnapVlm, RscLayerSuffixes.SUFFIX_DATA),
                        "internal"
                    );
                }
                session.metaDataDevices.put(drbdSnapVlm.getVlmNr().value, mdDev);
            }
        }
        catch (StorageException | DatabaseException | RuntimeException exc)
        {
            for (ReadOnlySnapshotHandle handle : session.handles)
            {
                handle.closeAfterFailure(exc);
            }
            throw exc;
        }
        errorReporter.logInfo(
            "CBT session: opened snapshot '%s' of resource '%s'",
            keyRef.snapName.displayValue,
            keyRef.rscName.displayValue
        );
        return session;
    }

    /**
     * Opens the child of the DRBD snapshot volume with the given suffix and returns the readable device.
     */
    private String openChild(Session sessionRef, DrbdVlmData<Snapshot> drbdSnapVlmRef, String suffixRef)
        throws StorageException, DatabaseException
    {
        @Nullable VlmProviderObject<Snapshot> child = drbdSnapVlmRef.getChildBySuffix(suffixRef);
        if (child == null)
        {
            throw new ImplementationError(
                "DRBD snapshot volume " + drbdSnapVlmRef.getVlmNr().value + " has no child with suffix '" +
                    suffixRef + "'"
            );
        }
        ReadOnlySnapshotHandle handle = deviceHandlerProvider.get()
            .openSnapshotReadOnly(ReadOnlyOpener.forSnapshot(child, OPENER_SUFFIX));
        sessionRef.handles.add(handle);
        // the clone device path is shared by all openers, keep our own copy
        @Nullable String devicePath = child.getCloneDevicePath();
        if (devicePath == null)
        {
            throw new StorageException(
                "Opening the snapshot volume " + drbdSnapVlmRef.getVlmNr().value + " returned no device"
            );
        }
        return devicePath;
    }

    private Snapshot getSnapshot(SessionKey keyRef)
    {
        @Nullable NodeName localNodeName = controllerPeerConnector.getLocalNodeName();
        @Nullable ResourceDefinition rscDfn = rscDfnMap.get(keyRef.rscName);
        @Nullable SnapshotDefinition snapDfn = rscDfn == null ? null : rscDfn.getSnapshotDfn(keyRef.snapName);
        @Nullable Snapshot snap = null;
        if (snapDfn != null && localNodeName != null && snapDfn.getFlags().isSet(SnapshotDefinition.Flags.SUCCESSFUL))
        {
            snap = snapDfn.getSnapshot(localNodeName);
        }
        if (snap == null || snap.getTakeSnapshot() || snap.getFlags().isSet(Snapshot.Flags.DELETE))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN,
                    "Snapshot '" + keyRef.snapName.displayValue + "' of resource '" + keyRef.rscName.displayValue +
                        "' is not available on this node"
                )
            );
        }
        return snap;
    }

    private CbtExtents runDump(
        MetaDataDevice mdDevRef,
        long startOffsetRef,
        int maxExtentsRef,
        ExtentsMode modeRef
    )
        throws StorageException
    {
        String[] cmd = {
            DrbdAdm.DRBDMETA_UTIL,
            "-",
            "v09",
            mdDevRef.path,
            mdDevRef.mdType,
            "cbt-dump",
            modeRef == ExtentsMode.DIRTY ? "--mode=dirty" : "--mode=zero",
            "--start-offset",
            Long.toString(startOffsetRef),
            "--max-extents",
            Integer.toString(maxExtentsRef)
        };
        ExtCmd extCmd = extCmdFactory.create().logExecution(false);
        extCmd.setTimeout(TimeoutType.WAIT, DUMP_TIMEOUT_MS);
        OutputData outputData;
        try
        {
            outputData = extCmd.exec(cmd);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException("Failed to run " + String.join(" ", cmd), exc);
        }
        if (outputData.exitCode == CBT_DUMP_NO_TRACKING_RC)
        {
            throw new ApiRcException(
                modeRef == ExtentsMode.DIRTY ?
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_CBT_FULL_BACKUP_REQUIRED,
                        "The snapshot holds no changes to base an incremental backup on"
                    )
                        .setCause(
                            "Its DRBD metadata has no trustworthy changed block tracking interval, e.g. because it " +
                                "is the first CBT snapshot of the volume."
                        )
                        .setCorrection("Make a full backup.")
                        .setSkipErrorReport(true)
                        .build() :
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_CBT_FULL_BACKUP_REQUIRED,
                        "The snapshot does not tell which ranges were never written"
                    )
                        .setCause(
                            "Its DRBD metadata was not flushed and its activity log is disabled or unreadable."
                        )
                        .setCorrection("Read the whole volume.")
                        .setSkipErrorReport(true)
                        .build()
            );
        }
        if (outputData.exitCode != 0)
        {
            throw new StorageException(
                "drbdmeta cbt-dump failed with exit code " + outputData.exitCode,
                null,
                new String(outputData.stderrData, StandardCharsets.UTF_8),
                null,
                "Command: " + String.join(" ", cmd)
            );
        }
        return CbtExtents.parse(new String(outputData.stdoutData, StandardCharsets.UTF_8), modeRef);
    }

    public record SessionKey(ResourceName rscName, SnapshotName snapName)
    {
    }

    /**
     * The device holding a snapshot volume's DRBD metadata, as drbdmeta's meta-data argument expects it.
     */
    private record MetaDataDevice(String path, String mdType)
    {
    }

    /**
     * What an extents request reports, as {@code drbdmeta cbt-dump --mode} does.
     */
    public enum ExtentsMode
    {
        /** changed since the previous CBT snapshot, and never written */
        DIRTY,
        /** never written, without the changed block tracking */
        ZERO
    }

    /**
     * A range of a volume in bytes. Dirty: changed since the previous CBT snapshot, null in zero mode. Zero: never
     * written since the DRBD metadata was created.
     */
    public record Extent(long start, long length, @Nullable Boolean dirty, boolean zero)
    {
    }

    /**
     * One dump of a volume, its extents cover the volume from the start offset on without gaps.
     */
    public record CbtExtents(
        @Nullable String baseCbtId,
        @Nullable String since,
        long volumeSize,
        List<Extent> extents,
        @Nullable Long nextOffset
    )
    {
        /**
         * Parses the output of {@code drbdmeta cbt-dump}.
         */
        static CbtExtents parse(String jsonRef, ExtentsMode modeRef) throws StorageException
        {
            try
            {
                JsonNode root = OBJ_MAPPER.readTree(jsonRef);
                checkHeader(root, jsonRef, modeRef);
                @Nullable JsonNode idNode = root.get("id");
                @Nullable JsonNode sinceNode = root.get("since");
                @Nullable JsonNode nextOffsetNode = root.get("next-offset");
                JsonNode extentsNode = root.get("extents");
                List<Extent> extents = new ArrayList<>(extentsNode.size());
                for (JsonNode extent : extentsNode)
                {
                    extents.add(parseExtent(extent, modeRef));
                }
                return new CbtExtents(
                    idNode == null ? null : idNode.asText(),
                    sinceNode == null ? null : sinceNode.asText(),
                    root.get("volume-size").asLong(),
                    extents,
                    nextOffsetNode == null ? null : nextOffsetNode.asLong()
                );
            }
            catch (JsonProcessingException exc)
            {
                throw new StorageException(
                    "Unable to parse the output of drbdmeta cbt-dump: " + abbreviate(jsonRef),
                    exc
                );
            }
        }

        /** checks the fields of the dump besides the extents */
        private static void checkHeader(@Nullable JsonNode rootRef, String jsonRef, ExtentsMode modeRef)
            throws StorageException
        {
            if (rootRef == null || rootRef.get("volume-size") == null || rootRef.get("extents") == null ||
                !rootRef.get("extents").isArray() || (modeRef == ExtentsMode.DIRTY && rootRef.get("id") == null))
            {
                throw new StorageException("Unexpected output of drbdmeta cbt-dump: " + abbreviate(jsonRef));
            }
            @Nullable JsonNode formatNode = rootRef.get("format");
            if (formatNode == null || formatNode.asInt() != CBT_DUMP_FORMAT)
            {
                throw new StorageException("Unknown format of drbdmeta cbt-dump: " + abbreviate(jsonRef));
            }
        }

        private static Extent parseExtent(JsonNode extentRef, ExtentsMode modeRef) throws StorageException
        {
            @Nullable JsonNode startNode = extentRef.get("start");
            @Nullable JsonNode lengthNode = extentRef.get("length");
            @Nullable JsonNode dirtyNode = extentRef.get("dirty");
            @Nullable JsonNode zeroNode = extentRef.get("zero");
            if (startNode == null || lengthNode == null || zeroNode == null ||
                (modeRef == ExtentsMode.DIRTY && dirtyNode == null))
            {
                throw new StorageException("Unexpected extent in the output of drbdmeta cbt-dump: " + extentRef);
            }
            return new Extent(
                startNode.asLong(),
                lengthNode.asLong(),
                modeRef == ExtentsMode.DIRTY ? dirtyNode.asBoolean() : null,
                zeroNode.asBoolean()
            );
        }

        private static String abbreviate(String strRef)
        {
            return strRef.length() > ABBREVIATE_LEN ? strRef.substring(0, ABBREVIATE_LEN) + "..." : strRef;
        }
    }

    private static class Session
    {
        private final SessionKey key;
        private final List<ReadOnlySnapshotHandle> handles = new ArrayList<>();
        private final Map<Integer, MetaDataDevice> metaDataDevices = new HashMap<>();
        private boolean closed = false;

        private Session(SessionKey keyRef)
        {
            key = keyRef;
        }

        /**
         * Closes all handles, also if closing one of them fails.
         */
        private void close() throws StorageException
        {
            closed = true;
            @Nullable StorageException firstExc = null;
            for (ReadOnlySnapshotHandle handle : handles)
            {
                try
                {
                    handle.close();
                }
                catch (StorageException exc)
                {
                    if (firstExc == null)
                    {
                        firstExc = exc;
                    }
                    else
                    {
                        firstExc.addSuppressed(exc);
                    }
                }
            }
            handles.clear();
            if (firstExc != null)
            {
                throw firstExc;
            }
        }
    }
}
