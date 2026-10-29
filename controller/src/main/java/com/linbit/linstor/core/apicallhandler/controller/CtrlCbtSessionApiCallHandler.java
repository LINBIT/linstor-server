package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.ResourceWithPayloadPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass.ApiCallResponse;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntReqCbtExtentsOuterClass.MsgIntReqCbtExtents;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntReqCbtSessionOuterClass.MsgIntReqCbtSession;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntCbtExtentsOuterClass.MsgIntCbtExtents;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntCbtSessionOuterClass.MsgIntCbtSession;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.google.protobuf.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * CBT sessions: a backup integration reads a CBT snapshot ({@link SnapshotDefinition.Flags#CBT}) through a session.
 * A session has two independent parts:
 * <ul>
 *   <li>The data path: the snapshot restored into a temporary resource definition ("&lt;rsc&gt;_cbt_&lt;hex&gt;")
 *     with one diskful replica on a node holding the snapshot (the "snapshot node") and, if the integration runs
 *     elsewhere, a diskless replica on the integration's node (the "session node"). The integration reads the DRBD
 *     devices of the session node, which the satellites set read-only. Nothing reads the metadata of the temporary
 *     resource; the restore stops the tracking it copied from the snapshot.</li>
 *   <li>The extents: the snapshot node opens the snapshot below DRBD read-only (as a clone opens its source
 *     snapshot) and keeps it open for the whole session. Each extents request runs {@code drbdmeta cbt-dump} on the
 *     DRBD metadata in the opened snapshot, which holds the tracking bitmap {@code cbt-flush} wrote right before the
 *     snapshot was taken. The extents cover the volume without gaps, as the extents of ovirt-imageio do: the dirty
 *     ones changed since the previous CBT snapshot (dirty mode, for incremental backups), the zero ones were never
 *     written (both modes; zero mode works without a tracking interval, for full backups).</li>
 * </ul>
 *
 * <p>Lifecycle:</p>
 * <ol>
 *   <li>{@link #startSession}: checks the snapshot and selects the snapshot node, creates the temporary resource
 *     definition with the session marks (see below) in one transaction, deploys it ({@code deploySession}) and
 *     flags it {@link ResourceDefinition.Flags#PREVENT_RESOURCE_CREATION} (a further replica would just copy the
 *     snapshot by a full sync), sends the open request to the snapshot node and returns the session as
 *     {@link #showSession} does. If anything after the transaction fails, the session is cleaned up as by
 *     {@link #finishSession}.</li>
 *   <li>{@link #showSession}: the temporary resource, its devices and sizes, and the snapshot's bookmark id.</li>
 *   <li>{@link #getExtents}: forwarded to the snapshot node, one page per request. If that satellite lost its open
 *     snapshot in between (restart or reconnect, it closes all sessions with its full sync), it reopens it.</li>
 *   <li>{@link #finishSession}: sends the close request to all nodes holding the snapshot, deletes the temporary
 *     resource definition and removes the session mark from the snapshot definition. If the deletion fails, the mark
 *     stays, so that finishing can be retried.</li>
 * </ol>
 *
 * <p>State, all in {@link InternalApiConsts#NAMESPC_INTERNAL_CBT}:</p>
 * <ul>
 *   <li>Snapshot definition {@link InternalApiConsts#KEY_CBT_SESSION}: the name of the temporary resource
 *     definition. It marks the snapshot as read by a session: one session per snapshot, and the snapshot cannot be
 *     deleted before the session is finished.</li>
 *   <li>Temporary resource definition {@link InternalApiConsts#KEY_CBT_SESSION_SNAPSHOT} and
 *     {@link InternalApiConsts#KEY_CBT_SESSION_SOURCE_RSC}: the snapshot it is restored from. The snapshot's name
 *     also marks the resource definition as temporary resource of a session ({@link #isCbtSessionRscDfn}): the
 *     schedule and auto-snapshot services skip it and the satellites set its devices read-only.
 *     {@link InternalApiConsts#KEY_CBT_SESSION_NODE} and {@link InternalApiConsts#KEY_CBT_SESSION_SNAPSHOT_NODE}:
 *     the session and snapshot nodes. {@link InternalApiConsts#KEY_CBT_SESSION_STARTED}: the start time.</li>
 * </ul>
 *
 * <p>The temporary resource definition can be deleted or broken behind the session's back (e.g. a replica deleted):
 * show and extents requests then fail, finishing still cleans up (the close request does not depend on it).</p>
 */
@Singleton
public class CtrlCbtSessionApiCallHandler
{
    public static final int DEFAULT_MAX_EXTENTS = 10_000;
    public static final String EXTENTS_MODE_DIRTY = "dirty";
    public static final String EXTENTS_MODE_ZERO = "zero";

    private static final String TMP_RSC_INFIX = "_cbt_";
    private static final int TMP_RSC_SOURCE_NAME_LEN = 36;
    /** 6 hex digits of random suffix */
    private static final int TMP_RSC_SUFFIX_BOUND = 0x1000000;
    private static final long KIB = 1024;
    private static final String VAL_DRBD_QUORUM_OFF = "off";

    /** namespaces of the source's properties that would make LINSTOR act on the temporary resource on its own */
    private static final List<String> STRIPPED_NAMESPACES = List.of(
        ApiConsts.NAMESPC_AUTO_SNAPSHOT,
        InternalApiConsts.NAMESPC_SCHEDULE,
        ApiConsts.NAMESPC_BACKUP_SHIPPING,
        ApiConsts.NAMESPC_AUTOPLACER
    );

    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSnapshotHelper ctrlSnapshotHelper;
    private final CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandler;
    private final CtrlSnapshotRestoreVlmDfnApiCallHandler ctrlSnapshotRestoreVlmDfnApiCallHandler;
    private final CtrlSnapshotRestoreApiCallHandler ctrlSnapshotRestoreApiCallHandler;
    private final CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandler;
    private final CtrlRscDfnDeleteApiCallHandler ctrlRscDfnDeleteApiCallHandler;

    @Inject
    public CtrlCbtSessionApiCallHandler(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandlerRef,
        CtrlSnapshotRestoreVlmDfnApiCallHandler ctrlSnapshotRestoreVlmDfnApiCallHandlerRef,
        CtrlSnapshotRestoreApiCallHandler ctrlSnapshotRestoreApiCallHandlerRef,
        CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandlerRef,
        CtrlRscDfnDeleteApiCallHandler ctrlRscDfnDeleteApiCallHandlerRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlRscDfnApiCallHandler = ctrlRscDfnApiCallHandlerRef;
        ctrlSnapshotRestoreVlmDfnApiCallHandler = ctrlSnapshotRestoreVlmDfnApiCallHandlerRef;
        ctrlSnapshotRestoreApiCallHandler = ctrlSnapshotRestoreApiCallHandlerRef;
        ctrlRscCrtApiCallHandler = ctrlRscCrtApiCallHandlerRef;
        ctrlRscDfnDeleteApiCallHandler = ctrlRscDfnDeleteApiCallHandlerRef;
    }

    /**
     * Whether the resource definition is the temporary resource of a CBT session.
     */
    public static boolean isCbtSessionRscDfn(ResourceDefinition rscDfnRef)
    {
        return getProp(rscDfnRef.getProps(), InternalApiConsts.KEY_CBT_SESSION_SNAPSHOT) != null;
    }

    public Mono<CbtSessionInfo> startSession(String rscNameStr, String snapNameStr, String nodeNameStr)
    {
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        SnapshotName snapName = LinstorParsingUtils.asSnapshotName(snapNameStr);
        return scopeRunner.fluxInTransactionalScope(
            "Prepare CBT session",
            lockGuardFactory.createDeferred().read(LockObj.NODES_MAP).write(LockObj.RSC_DFN_MAP).build(),
            () -> Flux.just(prepareSessionInTransaction(rscName, snapName, nodeNameStr))
        )
            .next()
            .flatMap(
                prepared -> deploySession(prepared)
                    .then(sendSessionRequest(prepared.snapNode, InternalApiConsts.API_REQUEST_CBT_SESSION_OPEN,
                        rscName, snapName))
                    .then(showSession(rscNameStr, snapNameStr))
                    .onErrorResume(
                        exc -> cleanupSession(rscName, snapName)
                            .onErrorResume(cleanupExc ->
                            {
                                errorReporter.reportError(cleanupExc);
                                return Flux.empty();
                            })
                            .then(Mono.error(exc))
                    )
            );
    }

    public Mono<CbtSessionInfo> showSession(String rscNameStr, String snapNameStr)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Show CBT session",
            lockGuardFactory.createDeferred().read(LockObj.NODES_MAP, LockObj.RSC_DFN_MAP).build(),
            () -> Flux.just(getSessionInfo(rscNameStr, snapNameStr))
        ).next();
    }

    /**
     * Requests one page of the extents of a session's snapshot volume from the snapshot node.
     *
     * @param modeStr {@link #EXTENTS_MODE_DIRTY} or {@link #EXTENTS_MODE_ZERO}, case insensitive
     */
    public Mono<CbtExtentsInfo> getExtents(
        String rscNameStr,
        String snapNameStr,
        int vlmNr,
        long startOffset,
        int maxExtents,
        String modeStr
    )
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Get CBT extents",
            lockGuardFactory.createDeferred().read(LockObj.NODES_MAP, LockObj.RSC_DFN_MAP).build(),
            () -> requestExtents(rscNameStr, snapNameStr, vlmNr, startOffset, maxExtents, modeStr)
        ).next();
    }

    public Flux<ApiCallRc> finishSession(String rscNameStr, String snapNameStr)
    {
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        SnapshotName snapName = LinstorParsingUtils.asSnapshotName(snapNameStr);
        return scopeRunner.fluxInTransactionlessScope(
            "Finish CBT session",
            lockGuardFactory.createDeferred().read(LockObj.NODES_MAP, LockObj.RSC_DFN_MAP).build(),
            () ->
            {
                // fails if there is no session
                getSessionRscName(loadSnapDfn(rscName, snapName));
                return Flux.<ApiCallRc>empty();
            }
        )
            .concatWith(cleanupSession(rscName, snapName))
            .concatWith(
                Flux.just(
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.DELETED,
                        "CBT session of snapshot '" + snapNameStr + "' of resource '" + rscNameStr + "' finished."
                    )
                )
            );
    }

    private PreparedSession prepareSessionInTransaction(
        ResourceName rscName,
        SnapshotName snapName,
        String nodeNameStr
    )
    {
        try
        {
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName);
            SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscDfn, snapName);
            Node node = ctrlApiDataLoader.loadNode(nodeNameStr);

            if (!snapDfn.getFlags().isSet(SnapshotDefinition.Flags.CBT))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_CBT_FULL_BACKUP_REQUIRED,
                        describe(rscName, snapName) + " is not a CBT snapshot."
                    )
                        .setCorrection("Make a full backup.")
                        .setSkipErrorReport(true)
                        .build()
                );
            }
            ctrlSnapshotHelper.ensureSnapshotSuccessful(snapDfn);
            @Nullable String existingSession = getProp(snapDfn.getSnapDfnProps(), InternalApiConsts.KEY_CBT_SESSION);
            if (existingSession != null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        describe(rscName, snapName) + " already has a CBT session (resource '" + existingSession +
                            "')."
                    )
                        .setSkipErrorReport(true)
                        .build()
                );
            }
            if (!node.getPeer().isOnline())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_CONNECTED,
                        "Node '" + nodeNameStr + "' is not connected.",
                        true
                    )
                );
            }
            final Node snapNode = selectSnapshotNode(snapDfn, node);

            ResourceName tmpRscName = generateTmpRscName(rscName);
            List<String> layerStack = new ArrayList<>();
            for (DeviceLayerKind kind : rscDfn.getLayerStack())
            {
                layerStack.add(kind.name());
            }
            ApiCallRcImpl responses = new ApiCallRcImpl();
            ResourceDefinition tmpRscDfn = ctrlRscDfnApiCallHandler.createResourceDefinition(
                tmpRscName.displayValue,
                null,
                Collections.emptyMap(),
                Collections.emptyList(),
                layerStack,
                null,
                rscDfn.getResourceGroup().getName().displayValue,
                true,
                responses,
                false
            );
            if (tmpRscDfn == null)
            {
                throw new ApiRcException(responses);
            }
            ctrlSnapshotRestoreVlmDfnApiCallHandler.restoreVlmDfnsInTransaction(snapDfn, tmpRscDfn, responses);

            snapDfn.getSnapDfnProps().setProp(
                InternalApiConsts.KEY_CBT_SESSION,
                tmpRscName.displayValue,
                InternalApiConsts.NAMESPC_INTERNAL_CBT
            );
            Props tmpProps = tmpRscDfn.getProps();
            setCbtProp(tmpProps, InternalApiConsts.KEY_CBT_SESSION_SOURCE_RSC, rscName.displayValue);
            setCbtProp(tmpProps, InternalApiConsts.KEY_CBT_SESSION_SNAPSHOT, snapName.displayValue);
            setCbtProp(tmpProps, InternalApiConsts.KEY_CBT_SESSION_NODE, node.getName().displayValue);
            setCbtProp(tmpProps, InternalApiConsts.KEY_CBT_SESSION_SNAPSHOT_NODE, snapNode.getName().displayValue);
            setCbtProp(
                tmpProps,
                InternalApiConsts.KEY_CBT_SESSION_STARTED,
                Long.toString(System.currentTimeMillis())
            );

            ctrlTransactionHelper.commit();
            return new PreparedSession(rscName, snapName, tmpRscName, snapNode.getName(), node.getName());
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    /**
     * The diskful replica of the temporary resource goes to the given node if it holds the snapshot, else to another
     * node holding it. The node needs the source resource (diskful) and has to be online.
     */
    private Node selectSnapshotNode(SnapshotDefinition snapDfn, Node preferredNode)
    {
        ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
        @Nullable Node ret = null;
        for (Snapshot snap : snapDfn.getAllSnapshots())
        {
            Node snapNode = snap.getNode();
            @Nullable Resource rsc = rscDfn.getResource(snapNode.getName());
            if (rsc != null && !rsc.isDrbdDiskless() && !snap.getFlags().isSet(Snapshot.Flags.DELETE) &&
                snapNode.getPeer().isOnline() && (ret == null || snapNode.equals(preferredNode)))
            {
                ret = snapNode;
            }
        }
        if (ret == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_CONNECTED,
                    "No online node holds " + describe(rscDfn.getName(), snapDfn.getName()) +
                        " together with a diskful replica of the resource.",
                    true
                )
            );
        }
        return ret;
    }

    private ResourceName generateTmpRscName(ResourceName rscName)
    {
        String base = rscName.displayValue.length() > TMP_RSC_SOURCE_NAME_LEN ?
            rscName.displayValue.substring(0, TMP_RSC_SOURCE_NAME_LEN) :
            rscName.displayValue;
        ResourceName ret;
        do
        {
            ret = LinstorParsingUtils.asRscName(
                base + TMP_RSC_INFIX + String.format("%06x", ThreadLocalRandom.current().nextInt(TMP_RSC_SUFFIX_BOUND))
            );
        }
        while (ctrlApiDataLoader.loadRscDfnOrNull(ret) != null);
        return ret;
    }

    /**
     * Restores the snapshot into the temporary resource definition, adds the diskless replica and then prevents
     * further resources in it. The flag cannot be set earlier: the restore clears it and it would also reject the
     * diskless replica.
     */
    private Mono<Void> deploySession(PreparedSession preparedRef)
    {
        Flux<ApiCallRc> flux = ctrlSnapshotRestoreApiCallHandler.restoreSnapshotForCbtSession(
            preparedRef.snapNode.displayValue,
            preparedRef.srcRscName,
            preparedRef.snapName,
            preparedRef.tmpRscName,
            this::prepareTmpRscDfn
        );
        if (!preparedRef.snapNode.equals(preparedRef.sessionNode))
        {
            flux = flux.concatWith(
                ctrlRscCrtApiCallHandler.createResource(
                    List.of(
                        new ResourceWithPayloadPojo(
                            new RscPojo(
                                preparedRef.tmpRscName.displayValue,
                                preparedRef.sessionNode.displayValue,
                                Resource.Flags.DRBD_DISKLESS.flagValue,
                                Collections.emptyMap()
                            ),
                            Collections.emptyList(),
                            null,
                            null,
                            null,
                            false
                        )
                    ),
                    null,
                    false,
                    Collections.emptyList(),
                    false
                )
            );
        }
        /*
         * Only the errors matter: the caller answers with the session itself, the success messages of the restore and
         * of the diskless resource creation are dropped.
         */
        return flux.collectList()
            .flatMap(rcList ->
            {
                ApiCallRcImpl merged = new ApiCallRcImpl();
                rcList.forEach(merged::addEntries);
                return merged.hasErrors() ? Mono.error(new ApiRcException(merged)) : Mono.empty();
            })
            .then(
                scopeRunner.fluxInTransactionalScope(
                    "Prevent resources of CBT session",
                    lockGuardFactory.createDeferred().write(LockObj.RSC_DFN_MAP).build(),
                    () -> preventResourceCreationInTransaction(preparedRef.tmpRscName)
                ).then()
            );
    }

    private Flux<ApiCallRc> preventResourceCreationInTransaction(ResourceName tmpRscNameRef)
    {
        try
        {
            ctrlApiDataLoader.loadRscDfn(tmpRscNameRef)
                .getFlags()
                .enableFlags(ResourceDefinition.Flags.PREVENT_RESOURCE_CREATION);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        ctrlTransactionHelper.commit();
        return Flux.empty();
    }

    /**
     * Removes or overrides what the restore copied from the source and would make LINSTOR act on the temporary
     * resource on its own (snapshots, shipping, placement, quorum, balancing). The temporary resource definition
     * stays in the source's resource group, so whatever the group or the controller could enable is set explicitly
     * on the resource definition. The schedule and auto-snapshot services skip temporary resources anyway, so their
     * namespaces are only removed.
     */
    private void prepareTmpRscDfn(ResourceDefinition tmpRscDfnRef)
        throws InvalidKeyException, InvalidValueException, DatabaseException
    {
        Props props = tmpRscDfnRef.getProps();
        for (String namespace : STRIPPED_NAMESPACES)
        {
            props.removeNamespace(namespace);
        }
        props.setProp(ApiConsts.KEY_BALANCE_AFTER_RESTORE, ApiConsts.VAL_FALSE, ApiConsts.NAMESPC_SNAPSHOT);
        props.setProp(ApiConsts.KEY_BALANCE_AFTER_CLONE, ApiConsts.VAL_FALSE, ApiConsts.NAMESPC_CLONE);
        // DRBD's read-only open on a Secondary needs quorum. Set by "the user", so auto-quorum leaves it alone.
        props.setProp(
            InternalApiConsts.KEY_DRBD_QUORUM,
            VAL_DRBD_QUORUM_OFF,
            ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS
        );
        props.setProp(
            ApiConsts.KEY_QUORUM_SET_BY,
            InternalApiConsts.SET_BY_VALUE_USER,
            ApiConsts.NAMESPC_INTERNAL_DRBD
        );
        props.setProp(ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER, ApiConsts.VAL_FALSE,
            ApiConsts.NAMESPC_DRBD_OPTIONS);
        props.setProp(ApiConsts.KEY_DRBD_AUTO_DISKFUL, "-1", ApiConsts.NAMESPC_DRBD_OPTIONS);
        props.setProp(ApiConsts.KEY_BALANCE_RESOURCES_ENABLED, ApiConsts.VAL_FALSE);
    }

    /**
     * Closes the snapshot, deletes the temporary resource definition (if it still exists) and removes the session mark
     * from the snapshot definition. If the deletion fails, the mark stays, so that the cleanup can be retried, and
     * the returned flux fails with the deletion's errors.
     */
    private Flux<ApiCallRc> cleanupSession(ResourceName rscName, SnapshotName snapName)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Clean up CBT session",
            lockGuardFactory.createDeferred().read(LockObj.NODES_MAP, LockObj.RSC_DFN_MAP).build(),
            () -> closeSnapshotAndDeleteTmpRscDfn(rscName, snapName)
        )
            .collectList()
            .flatMapMany(rcList ->
            {
                ApiCallRcImpl merged = new ApiCallRcImpl();
                rcList.forEach(merged::addEntries);
                return merged.hasErrors() ?
                    Flux.error(new ApiRcException(merged)) :
                    Flux.fromIterable(rcList).concatWith(removeSessionMark(rscName, snapName));
            });
    }

    private Flux<ApiCallRc> closeSnapshotAndDeleteTmpRscDfn(ResourceName rscName, SnapshotName snapName)
    {
        SnapshotDefinition snapDfn = loadSnapDfn(rscName, snapName);
        @Nullable String tmpRscNameStr = getProp(snapDfn.getSnapDfnProps(), InternalApiConsts.KEY_CBT_SESSION);
        Flux<ApiCallRc> flux = Flux.empty();
        if (tmpRscNameStr != null)
        {
            /*
             * Only the temporary resource definition knows the snapshot node, but it might have been deleted already.
             * Closing is a no-op on a node without the session, so all nodes holding the snapshot get the request.
             * Offline nodes close their sessions when they reconnect.
             */
            for (Snapshot snap : snapDfn.getAllSnapshots())
            {
                Node node = snap.getNode();
                if (node.getPeer().isOnline())
                {
                    flux = flux.concatWith(closeSnapshot(node.getName(), rscName, snapName));
                }
            }
            if (ctrlApiDataLoader.loadRscDfnOrNull(tmpRscNameStr) != null)
            {
                // reports its failures as error entries
                flux = flux.concatWith(ctrlRscDfnDeleteApiCallHandler.deleteResourceDefinition(tmpRscNameStr));
            }
        }
        return flux;
    }

    private Flux<ApiCallRc> closeSnapshot(NodeName nodeName, ResourceName rscName, SnapshotName snapName)
    {
        return sendSessionRequest(nodeName, InternalApiConsts.API_REQUEST_CBT_SESSION_CLOSE, rscName, snapName)
            .onErrorResume(ApiRcException.class, exc ->
            {
                // a node that went offline closes its sessions when it reconnects, and a satellite forgets the
                // session also if closing it failed: keeping the session does not help
                errorReporter.reportError(exc);
                return Mono.empty();
            })
            .thenMany(Flux.empty());
    }

    private Flux<ApiCallRc> removeSessionMark(ResourceName rscName, SnapshotName snapName)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Remove CBT session",
            lockGuardFactory.createDeferred().write(LockObj.RSC_DFN_MAP).build(),
            () ->
            {
                @Nullable SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfnOrNull(
                    rscName.displayValue,
                    snapName.displayValue
                );
                if (snapDfn != null)
                {
                    snapDfn.getSnapDfnProps().removeProp(
                        InternalApiConsts.KEY_CBT_SESSION,
                        InternalApiConsts.NAMESPC_INTERNAL_CBT
                    );
                    ctrlTransactionHelper.commit();
                }
                return Flux.<ApiCallRc>empty();
            }
        );
    }

    private CbtSessionInfo getSessionInfo(String rscNameStr, String snapNameStr)
    {
        SnapshotDefinition snapDfn = loadSnapDfn(
            LinstorParsingUtils.asRscName(rscNameStr),
            LinstorParsingUtils.asSnapshotName(snapNameStr)
        );
        String tmpRscNameStr = getSessionRscName(snapDfn);
        ResourceDefinition tmpRscDfn = loadTmpRscDfn(tmpRscNameStr);
        ReadOnlyProps tmpProps = tmpRscDfn.getProps();
        String sessionNodeStr = getRequiredProp(tmpProps, InternalApiConsts.KEY_CBT_SESSION_NODE);
        String snapNodeStr = getRequiredProp(tmpProps, InternalApiConsts.KEY_CBT_SESSION_SNAPSHOT_NODE);

        /*
         * The integration reads the devices of the session node's replica (sessionRsc), which reads the data of the
         * snapshot node's diskful replica (snapRsc, the same resource if both nodes are the same). The diskful replica
         * is the only one holding the restored data: without either of them (or a volume of them) the session has no
         * data path anymore and cannot be repaired, a new session has to be started.
         */
        @Nullable Resource sessionRsc = tmpRscDfn.getResource(LinstorParsingUtils.asNodeName(sessionNodeStr));
        @Nullable Resource snapRsc = tmpRscDfn.getResource(LinstorParsingUtils.asNodeName(snapNodeStr));
        if (sessionRsc == null || snapRsc == null)
        {
            throw sessionBroken(tmpRscNameStr, "a replica of it was deleted");
        }
        List<CbtSessionVolumeInfo> volumes = new ArrayList<>();
        Iterator<Volume> vlmIt = sessionRsc.iterateVolumes();
        while (vlmIt.hasNext())
        {
            Volume vlm = vlmIt.next();
            @Nullable Volume snapVlm = snapRsc.getVolume(vlm.getVolumeNumber());
            if (snapVlm == null)
            {
                throw sessionBroken(tmpRscNameStr, "volume " + vlm.getVolumeNumber().value + " of it was deleted");
            }
            /*
             * The readable size is the usable size of the diskful replica's DRBD device (DRBD hides its metadata),
             * as its satellite reported it. The volume definition's size is the fallback until the report arrived.
             */
            long sizeKib = snapVlm.isUsableSizeSet() ?
                snapVlm.getUsableSize() :
                vlm.getVolumeDefinition().getVolumeSize();
            volumes.add(new CbtSessionVolumeInfo(vlm.getVolumeNumber().value, vlm.getDevicePath(), sizeKib * KIB));
        }
        return new CbtSessionInfo(
            tmpRscNameStr,
            sessionNodeStr,
            getRequiredProp(snapDfn.getSnapDfnProps(), ApiConsts.KEY_CBT_ID, ApiConsts.NAMESPC_CBT),
            volumes
        );
    }

    private Flux<CbtExtentsInfo> requestExtents(
        String rscNameStr,
        String snapNameStr,
        int vlmNr,
        long startOffset,
        int maxExtents,
        String modeStr
    )
    {
        if (startOffset < 0 || maxExtents < 1 || maxExtents > InternalApiConsts.CBT_MAX_EXTENTS_LIMIT)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REQUEST,
                    "start_offset must not be negative and max_extents must be between 1 and " +
                        InternalApiConsts.CBT_MAX_EXTENTS_LIMIT + ".",
                    true
                )
            );
        }
        MsgIntReqCbtExtents.Mode mode;
        if (EXTENTS_MODE_DIRTY.equalsIgnoreCase(modeStr))
        {
            mode = MsgIntReqCbtExtents.Mode.DIRTY;
        }
        else if (EXTENTS_MODE_ZERO.equalsIgnoreCase(modeStr))
        {
            mode = MsgIntReqCbtExtents.Mode.ZERO;
        }
        else
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REQUEST,
                    "mode must be '" + EXTENTS_MODE_DIRTY + "' or '" + EXTENTS_MODE_ZERO + "', not '" + modeStr + "'.",
                    true
                )
            );
        }
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        SnapshotName snapName = LinstorParsingUtils.asSnapshotName(snapNameStr);
        SnapshotDefinition snapDfn = loadSnapDfn(rscName, snapName);
        String tmpRscNameStr = getSessionRscName(snapDfn);
        if (snapDfn.getSnapshotVolumeDefinition(LinstorParsingUtils.asVlmNr(vlmNr)) == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_VLM_DFN,
                    describe(rscName, snapName) + " has no volume " + vlmNr + ".",
                    true
                )
            );
        }
        NodeName snapNodeName = LinstorParsingUtils.asNodeName(
            getRequiredProp(loadTmpRscDfn(tmpRscNameStr).getProps(), InternalApiConsts.KEY_CBT_SESSION_SNAPSHOT_NODE)
        );
        Peer peer = ctrlApiDataLoader.loadNode(snapNodeName).getPeer();
        byte[] reqMsg = toBytes(
            MsgIntReqCbtExtents.newBuilder()
                .setRscName(rscName.displayValue)
                .setSnapName(snapName.displayValue)
                .setVlmNr(vlmNr)
                .setStartOffset(startOffset)
                .setMaxExtents(maxExtents)
                .setMode(mode)
                .build()
        );
        return peer.apiCall(InternalApiConsts.API_REQUEST_CBT_EXTENTS, reqMsg)
            .onErrorMap(PeerNotConnectedException.class, exc -> notConnected(snapNodeName))
            .switchIfEmpty(Flux.error(() -> notConnected(snapNodeName)))
            .map(this::parseExtents);
    }

    private CbtExtentsInfo parseExtents(ByteArrayInputStream dataIn)
    {
        MsgIntCbtExtents msg;
        try
        {
            msg = MsgIntCbtExtents.parseDelimitedFrom(dataIn);
        }
        catch (IOException exc)
        {
            throw new ImplementationError(exc);
        }
        throwIfErrors(msg.getErrorsList());
        // no dirty flags in zero mode
        boolean hasDirty = msg.getDirtyCount() > 0;
        List<CbtExtentInfo> extents = new ArrayList<>(msg.getStartsCount());
        for (int idx = 0; idx < msg.getStartsCount(); ++idx)
        {
            extents.add(
                new CbtExtentInfo(
                    msg.getStarts(idx),
                    msg.getLengths(idx),
                    hasDirty ? msg.getDirty(idx) : null,
                    msg.getZero(idx)
                )
            );
        }
        return new CbtExtentsInfo(
            msg.getBaseCbtId().isEmpty() ? null : msg.getBaseCbtId(),
            msg.getSince().isEmpty() ? null : msg.getSince(),
            msg.getVolumeSize(),
            extents,
            msg.hasNextOffset() ? msg.getNextOffset() : null
        );
    }

    /**
     * Sends an open or close request for the session's snapshot to the node holding it and fails with the
     * satellite's errors.
     */
    private Mono<Void> sendSessionRequest(
        NodeName snapNodeName,
        String apiCallName,
        ResourceName rscName,
        SnapshotName snapName
    )
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Send CBT session request",
            lockGuardFactory.createDeferred().read(LockObj.NODES_MAP).build(),
            () ->
            {
                Peer peer = ctrlApiDataLoader.loadNode(snapNodeName).getPeer();
                byte[] reqMsg = toBytes(
                    MsgIntReqCbtSession.newBuilder()
                        .setRscName(rscName.displayValue)
                        .setSnapName(snapName.displayValue)
                        .build()
                );
                return peer.apiCall(apiCallName, reqMsg)
                    .onErrorMap(PeerNotConnectedException.class, exc -> notConnected(snapNodeName))
                    .switchIfEmpty(Flux.error(() -> notConnected(snapNodeName)))
                    .map(dataIn ->
                    {
                        try
                        {
                            throwIfErrors(MsgIntCbtSession.parseDelimitedFrom(dataIn).getErrorsList());
                        }
                        catch (IOException exc)
                        {
                            throw new ImplementationError(exc);
                        }
                        return dataIn;
                    });
            }
        ).then();
    }

    private static void throwIfErrors(List<ApiCallResponse> errorsRef)
    {
        if (!errorsRef.isEmpty())
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            for (ApiCallResponse msgApiCallResponse : errorsRef)
            {
                apiCallRc.addEntry(ProtoDeserializationUtils.parseApiCallRc(msgApiCallResponse, ""));
            }
            throw new ApiRcException(apiCallRc);
        }
    }

    private static byte[] toBytes(Message msgRef)
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            msgRef.writeDelimitedTo(baos);
            return baos.toByteArray();
        }
        catch (IOException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private SnapshotDefinition loadSnapDfn(ResourceName rscName, SnapshotName snapName)
    {
        return ctrlApiDataLoader.loadSnapshotDfn(ctrlApiDataLoader.loadRscDfn(rscName), snapName);
    }

    private String getSessionRscName(SnapshotDefinition snapDfn)
    {
        @Nullable String tmpRscNameStr = getProp(snapDfn.getSnapDfnProps(), InternalApiConsts.KEY_CBT_SESSION);
        if (tmpRscNameStr == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                    describe(snapDfn.getResourceName(), snapDfn.getName()) + " has no CBT session.",
                    true
                )
            );
        }
        return tmpRscNameStr;
    }

    private ResourceDefinition loadTmpRscDfn(String tmpRscNameStr)
    {
        @Nullable ResourceDefinition tmpRscDfn = ctrlApiDataLoader.loadRscDfnOrNull(tmpRscNameStr);
        if (tmpRscDfn == null)
        {
            throw sessionBroken(tmpRscNameStr, "it was deleted");
        }
        return tmpRscDfn;
    }

    private static ApiRcException sessionBroken(String tmpRscNameStr, String reasonRef)
    {
        return new ApiRcException(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                "The temporary resource '" + tmpRscNameStr + "' of the CBT session is unusable: " + reasonRef + "."
            )
                .setCorrection("Finish the CBT session and start a new one.")
                .setSkipErrorReport(true)
                .build()
        );
    }

    private static ApiRcException notConnected(NodeName nodeNameRef)
    {
        return new ApiRcException(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_NOT_CONNECTED,
                "Node '" + nodeNameRef.displayValue + "' holding the snapshot of the CBT session is not connected.",
                true
            )
        );
    }

    private static String describe(ResourceName rscName, SnapshotName snapName)
    {
        return "Snapshot '" + snapName.displayValue + "' of resource '" + rscName.displayValue + "'";
    }

    private static void setCbtProp(Props propsRef, String keyRef, String valueRef)
        throws InvalidKeyException, InvalidValueException, DatabaseException
    {
        propsRef.setProp(keyRef, valueRef, InternalApiConsts.NAMESPC_INTERNAL_CBT);
    }

    private static @Nullable String getProp(ReadOnlyProps propsRef, String keyRef)
    {
        return getProp(propsRef, keyRef, InternalApiConsts.NAMESPC_INTERNAL_CBT);
    }

    private static @Nullable String getProp(ReadOnlyProps propsRef, String keyRef, String namespaceRef)
    {
        try
        {
            return propsRef.getProp(keyRef, namespaceRef);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private static String getRequiredProp(ReadOnlyProps propsRef, String keyRef)
    {
        return getRequiredProp(propsRef, keyRef, InternalApiConsts.NAMESPC_INTERNAL_CBT);
    }

    private static String getRequiredProp(ReadOnlyProps propsRef, String keyRef, String namespaceRef)
    {
        @Nullable String value = getProp(propsRef, keyRef, namespaceRef);
        if (value == null)
        {
            throw new ImplementationError("Property " + namespaceRef + "/" + keyRef + " of a CBT session is missing");
        }
        return value;
    }

    private record PreparedSession(
        ResourceName srcRscName,
        SnapshotName snapName,
        ResourceName tmpRscName,
        NodeName snapNode,
        NodeName sessionNode
    )
    {
    }

    public record CbtSessionInfo(
        String rscName,
        String nodeName,
        String cbtId,
        List<CbtSessionVolumeInfo> volumes
    )
    {
    }

    public record CbtSessionVolumeInfo(int vlmNr, String devicePath, long sizeBytes)
    {
    }

    public record CbtExtentsInfo(
        @Nullable String baseCbtId,
        @Nullable String since,
        long volumeSize,
        List<CbtExtentInfo> extents,
        @Nullable Long nextOffset
    )
    {
    }

    /**
     * A range of a volume in bytes.
     *
     * @param dirty changed since the previous CBT snapshot, null in zero mode
     * @param zero never written since the DRBD metadata was created
     */
    public record CbtExtentInfo(long start, long length, @Nullable Boolean dirty, boolean zero)
    {
    }
}
