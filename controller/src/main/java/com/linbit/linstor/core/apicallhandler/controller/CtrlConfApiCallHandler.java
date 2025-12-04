package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.extproc.ChildProcessHandler;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.SatelliteConfig;
import com.linbit.linstor.backupshipping.BackupConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper.PropertyChangedListener;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IncorrectPassphraseException;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.MissingKeyPropertyException;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.controller.helpers.PropsChangedListenerBuilder;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlBackupQueueInternalCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDefinitionUtils;
import com.linbit.linstor.core.apicallhandler.controller.utils.ZfsDeleteStrategy;
import com.linbit.linstor.core.apicallhandler.controller.utils.ZfsRollbackStrategy;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.apis.ControllerConfigApi;
import com.linbit.linstor.core.apis.SatelliteConfigApi;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.AutoDiskfulTask;
import com.linbit.linstor.tasks.AutoSnapshotTask;
import com.linbit.linstor.tasks.BalanceResourcesTask;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.linstor.tasks.TaskScheduleService;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.StringUtils;
import com.linbit.utils.TripleNonNull;
import com.linbit.utils.UuidUtils;

import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;

import org.slf4j.MDC;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlConfApiCallHandler
{
    private static final int MAX_REMOTE_NAME_LEN = 10;

    private final ErrorReporter errorReporter;
    private final SystemConfRepository systemConfRepository;
    private final DynamicNumberPool minorNrPool;
    private final DynamicNumberPool backupShipPortPool;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<Peer> peerProvider;
    private final Provider<TransactionMgr> transMgrProvider;

    private final CtrlStltSerializer ctrlStltSrzl;
    private final NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final WhitelistProps whitelistProps;
    private final EncryptionHelper encHelper;
    private final CtrlConfig ctrlCfg;
    private final ScopeRunner scopeRunner;
    private final ResponseConverter responseConverter;
    private final CtrlNodeApiCallHandler ctrlNodeApiCallHandler;

    private final LockGuardFactory lockGuardFactory;
    private final AutoDiskfulTask autoDiskfulTask;
    private final ReconnectorTask reconnectorTask;
    private final CtrlRscDfnAutoVerifyAlgoHelper ctrlRscDfnAutoVerifyAlgoHelper;

    private final AutoSnapshotTask autoSnapshotTask;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteHandler;
    private final CtrlResyncAfterHelper ctrlResyncAfterHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;

    private final Provider<PropsChangedListenerBuilder> propsChangeListenerBuilder;

    private final CtrlBackupQueueInternalCallHandler ctrlBackupQueueHandler;
    private final TaskScheduleService taskScheduleService;
    private final BalanceResourcesTask balanceResourcesTask;

    private final CtrlRscAutoHelper ctrlRscAutoHelper;

    public enum LinstorEncryptionStatus
    {
        UNSET,
        LOCKED,
        UNLOCKED,
    }

    @FunctionalInterface
    private interface SpecialPropHandler
    {
        /**
         * This method is expected to delete the entries of the input map/sets once those are handled and should NOT be
         * passed through to the usual whitelisting mechanism
         */
        @Nullable
        ApiCallRc handle(
            HashMap<String, String> filteredOverrideProps,
            HashSet<String> filteredDeletePropKeys,
            HashSet<String> filteredDeleteNamespaces,
            Map<String, PropertyChangedListener> propertyChangedListeners
        );
    }

    @Inject
    public CtrlConfApiCallHandler(
        ErrorReporter errorReporterRef,
        SystemConfRepository systemConfRepositoryRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorNrPoolRef,
        @Named(
            NumberPoolModule.BACKUP_SHIPPING_PORT_POOL
        ) DynamicNumberPool backupShipPortPoolRef,
        @SystemContext AccessContext sysCtxRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerProviderRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.NodesMap nodesMapRef,
        CtrlStltSerializer ctrlStltSrzlRef,
        WhitelistProps whitelistPropsRef,
        EncryptionHelper encHelperRef,
        LockGuardFactory lockGuardFactoryRef,
        ScopeRunner scopeRunnerRef,
        CtrlConfig ctrlCfgRef,
        ResponseConverter responseConverterRef,
        CtrlNodeApiCallHandler ctrlNodeApiCallHandlerRef,
        AutoDiskfulTask autoDiskfulTaskRef,
        ReconnectorTask reconnectorTaskRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CtrlRscDfnAutoVerifyAlgoHelper ctrlRscDfnAutoVerifyAlgoHelperRef,
        AutoSnapshotTask autoSnapshotTaskRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteHandlerRef,
        CtrlResyncAfterHelper ctrlResyncAfterHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        Provider<PropsChangedListenerBuilder> propsChangeListenerBuilderRef,
        CtrlBackupQueueInternalCallHandler ctrlBackupQueueHandlerRef,
        TaskScheduleService taskScheduleServiceRef,
        BalanceResourcesTask balanceResourcesTaskRef,
        CtrlRscAutoHelper ctrlRscAutoHelperRef
    )
    {
        errorReporter = errorReporterRef;
        systemConfRepository = systemConfRepositoryRef;
        minorNrPool = minorNrPoolRef;
        backupShipPortPool = backupShipPortPoolRef;
        peerAccCtx = peerAccCtxRef;
        peerProvider = peerProviderRef;
        transMgrProvider = transMgrProviderRef;

        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        ctrlStltSrzl = ctrlStltSrzlRef;
        whitelistProps = whitelistPropsRef;
        encHelper = encHelperRef;
        lockGuardFactory = lockGuardFactoryRef;
        scopeRunner = scopeRunnerRef;
        ctrlCfg = ctrlCfgRef;
        responseConverter = responseConverterRef;
        ctrlNodeApiCallHandler = ctrlNodeApiCallHandlerRef;
        autoDiskfulTask = autoDiskfulTaskRef;
        reconnectorTask = reconnectorTaskRef;
        ctrlRscDfnAutoVerifyAlgoHelper = ctrlRscDfnAutoVerifyAlgoHelperRef;
        autoSnapshotTask = autoSnapshotTaskRef;
        ctrlSnapDeleteHandler = ctrlSnapDeleteHandlerRef;
        ctrlResyncAfterHelper = ctrlResyncAfterHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        propsChangeListenerBuilder = propsChangeListenerBuilderRef;
        ctrlBackupQueueHandler = ctrlBackupQueueHandlerRef;
        taskScheduleService = taskScheduleServiceRef;
        balanceResourcesTask = balanceResourcesTaskRef;
        ctrlRscAutoHelper = ctrlRscAutoHelperRef;
    }

    public void updateSatelliteConf() throws AccessDeniedException
    {
        for (Node nodeToContact : nodesMap.values())
        {
            Peer satellitePeer = nodeToContact.getPeer(peerAccCtx.get());

            if (satellitePeer.isOnline() && !satellitePeer.hasFullSyncFailed())
            {
                byte[] changedMessage = ctrlStltSrzl
                    .onewayBuilder(InternalApiConsts.API_CHANGED_CONTROLLER)
                    .build();

                satellitePeer.sendMessage(changedMessage);
            }
        }
    }

    public Flux<ApiCallRc> modifyCtrl(
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespacesRef
    )
    {
        ResponseContext context = makeCtrlConfContext(
            ApiOperation.makeModifyOperation()
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "modifyCtrl",
                lockGuardFactory.buildDeferred(WRITE, LockObj.CTRL_CONFIG),
                () -> modifyCtrlInTransaction(
                    overridePropsRef,
                    deletePropKeysRef,
                    deletePropNamespacesRef
                ),
                MDC.getCopyOfContextMap()
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> handleAutoQuorum(
        Collection<ResourceDefinition> rscDfns,
        Map<String, String> overrideProps, Set<String> deletePropKeys,
        ApiCallRcImpl apiCallRc)
    {
        Flux<ApiCallRc> flux = Flux.empty();

        String drbdQuorum = ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS + "/" + InternalApiConsts.KEY_DRBD_QUORUM;
        boolean drbdQuorumChanged = false;
        if (overrideProps.containsKey(drbdQuorum))
        {
            overrideProps.put(ApiConsts.NAMESPC_INTERNAL_DRBD + "/" + ApiConsts.KEY_QUORUM_SET_BY, "user");
            drbdQuorumChanged = true;
        }

        if (deletePropKeys.contains(drbdQuorum))
        {
            deletePropKeys.add(ApiConsts.NAMESPC_INTERNAL_DRBD + "/" + ApiConsts.KEY_QUORUM_SET_BY);
            drbdQuorumChanged = true;
        }

        // run auto quorum/tiebreaker manage code
        String autoTiebreakerKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/" +
            ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER;
        if (overrideProps.containsKey(autoTiebreakerKey) || deletePropKeys.contains(autoTiebreakerKey)
            || drbdQuorumChanged)
        {
            for (ResourceDefinition rscDfn : rscDfns)
            {
                ResponseContext context = CtrlRscDfnApiCallHandler.makeResourceDefinitionContext(
                    ApiOperation.makeModifyOperation(),
                    rscDfn.getName().displayValue
                );

                CtrlRscAutoQuorumHelper.removeQuorumPropIfSetByLinstor(rscDfn, peerAccCtx.get());
                ApiCallRcImpl responses = new ApiCallRcImpl();
                CtrlRscAutoHelper.AutoHelperContext autoHelperCtx =
                    new CtrlRscAutoHelper.AutoHelperContext(responses, context, rscDfn);
                ctrlRscAutoHelper.manage(
                    autoHelperCtx, new HashSet<>(Arrays.asList(
                        CtrlRscAutoHelper.AutoHelperType.AutoQuorum, CtrlRscAutoHelper.AutoHelperType.TieBreaker)));

                apiCallRc.addEntries(autoHelperCtx.responses);
                flux = flux.concatWith(Flux.merge(autoHelperCtx.additionalFluxList));
            }
        }

        return flux;
    }

    private Flux<ApiCallRc> modifyCtrlInTransaction(
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespacesRef
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        List<Flux<ApiCallRc>> specialPropFluxes = new ArrayList<>();
        Map<String, PropertyChangedListener> propsChangedListeners = propsChangeListenerBuilder.get()
            .buildPropsChangedListeners(peerAccCtx.get(), specialPropFluxes);

        /*
         * Some properties need to be considered in combination with other properties.
         * Those other properties might already exist, but might also be part of the current modification.
         *
         * Therefore we copy the input map/sets, filter them by special rules if necessary,
         * and the properties that were not filtered are passed to the usual whitelisting mechanism as before
         */
        HashMap<String, String> filteredOverrideProps = new HashMap<>(overridePropsRef);
        HashSet<String> filteredDeletePropKeys = new HashSet<>(deletePropKeysRef);
        HashSet<String> filteredDeleteNamespaces = new HashSet<>(deletePropNamespacesRef);

        // this list might get expanded so don't convert to Collections.singleton
        List<SpecialPropHandler> specialHandlers = Arrays.asList(this::handleNetComModifications);
        for (SpecialPropHandler specialHandler : specialHandlers)
        {
            ApiCallRc handlersApiCallRc = specialHandler.handle(
                filteredOverrideProps,
                filteredDeletePropKeys,
                filteredDeleteNamespaces,
                propsChangedListeners
            );
            if (handlersApiCallRc != null)
            {
                apiCallRc.addEntries(handlersApiCallRc);
            }
        }



        boolean notifyStlts = false;
        Flux<ApiCallRc> fluxUpdRscDfns = Flux.empty();
        Set<Resource> updateRscs = new HashSet<>();
        for (Entry<String, String> overrideProp : filteredOverrideProps.entrySet())
        {
            TripleNonNull<ApiCallRc, Boolean, Set<Resource>> result = setProp(
                overrideProp.getKey(),
                null,
                overrideProp.getValue(),
                propsChangedListeners
            );
            if (result.objA.hasErrors())
            {
                throw new ApiRcException(result.objA);
            }
            updateRscs.addAll(result.objC);
            apiCallRc.addEntries(result.objA);
            notifyStlts |= result.objB;
        }
        for (String deletePropKey : filteredDeletePropKeys)
        {
            TripleNonNull<ApiCallRc, Boolean, Set<Resource>> result = deleteProp(
                deletePropKey,
                null,
                propsChangedListeners
            );
            if (result.objA.hasErrors())
            {
                throw new ApiRcException(result.objA);
            }
            updateRscs.addAll(result.objC);
            apiCallRc.addEntries(result.objA);
            notifyStlts |= result.objB;
        }
        for (String deleteNamespace : filteredDeleteNamespaces)
        {
            // we should not simply "drop" the namespace here, as we might have special cleanup logic
            // for some of the deleted keys.
            PairNonNull<ApiCallRc, Boolean> result = deleteNamespace(deleteNamespace, propsChangedListeners);
            if (result.objA.hasErrors())
            {
                throw new ApiRcException(result.objA);
            }
            apiCallRc.addEntries(result.objA);
            notifyStlts |= result.objB;
        }

        for (Resource rsc : updateRscs)
        {
            fluxUpdRscDfns = fluxUpdRscDfns.concatWith(
                ctrlSatelliteUpdateCaller.updateSatellites(rsc, Flux.empty())
                    .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2()));
        }

        fluxUpdRscDfns = fluxUpdRscDfns.concatWith(handleAutoQuorum(
            rscDfnMap.values(), overridePropsRef, deletePropKeysRef, apiCallRc));

        Flux<ApiCallRc> autoSnapFlux;
        try
        {
            autoSnapFlux = ResourceDefinitionUtils.handleAutoSnapProps(
                autoSnapshotTask,
                ctrlSnapDeleteHandler,
                filteredOverrideProps,
                filteredDeletePropKeys,
                filteredDeleteNamespaces,
                Collections.unmodifiableCollection(rscDfnMap.values()),
                peerAccCtx.get(),
                systemConfRepository.getStltConfForView(peerAccCtx.get()),
                false
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "Checking props for auto-snapshot",
                ApiConsts.FAIL_ACC_DENIED_CTRL_CFG
            );
        }

        transMgrProvider.get().commit();

        Flux<ApiCallRc> updSatellites = Flux.empty();
        if (notifyStlts)
        {
            updSatellites = ctrlSatelliteUpdateCaller.updateSatellitesConf();
        }

        String autoDiskfulKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_DISKFUL;
        if (
            overridePropsRef.containsKey(autoDiskfulKey) || deletePropKeysRef.contains(autoDiskfulKey) ||
                deletePropNamespacesRef.contains(ApiConsts.NAMESPC_DRBD_OPTIONS)
        )
        {
            autoDiskfulTask.update();
        }

        boolean hasKeyInDrbdOptions = false;
        boolean maxConcurrentShippingsChanged = false;
        for (String key : overridePropsRef.keySet())
        {
            if (key.startsWith(ApiConsts.NAMESPC_DRBD_OPTIONS))
            {
                hasKeyInDrbdOptions = true;
            }
            else if (key.equals(BackupConsts.CONCURRENT_BACKUPS_KEY))
            {
                maxConcurrentShippingsChanged = true;
            }
        }
        for (String key : deletePropKeysRef)
        {
            if (key.startsWith(ApiConsts.NAMESPC_DRBD_OPTIONS))
            {
                hasKeyInDrbdOptions = true;
            }
            else if (key.equals(BackupConsts.CONCURRENT_BACKUPS_KEY))
            {
                maxConcurrentShippingsChanged = true;
            }
        }
        hasKeyInDrbdOptions |= deletePropNamespacesRef.contains(ApiConsts.NAMESPC_DRBD_OPTIONS);
        Flux<ApiCallRc> evictionFlux = Flux.empty();
        if (hasKeyInDrbdOptions)
        {
            ArrayList<PairNonNull<Flux<ApiCallRc>, Peer>> rerunConfigChecks = reconnectorTask.rerunConfigChecks();
            for (PairNonNull<Flux<ApiCallRc>, Peer> pair : rerunConfigChecks)
            {
                evictionFlux = evictionFlux.concatWith(pair.objA);
            }
        }
        Flux<ApiCallRc> shippingFlux = Flux.empty();
        if (maxConcurrentShippingsChanged)
        {
            shippingFlux = ctrlBackupQueueHandler.maxConcurrentShippingsChangedForCtrl();
        }

        return Flux.<ApiCallRc>just(apiCallRc)
            .concatWith(updSatellites)
            .concatWith(evictionFlux)
            .concatWith(autoSnapFlux)
            .concatWith(fluxUpdRscDfns)
            .concatWith(Flux.merge(specialPropFluxes))
            .concatWith(shippingFlux);
    }

    public Flux<ApiCallRc> setCtrlConfig(
        ControllerConfigApi config
    )
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "set controller config",
                lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.CTRL_CONFIG),
                () -> setCtrlConfigInScope(config)
            );
    }

    private Flux<ApiCallRc> setCtrlConfigInScope(ControllerConfigApi config)
        throws AccessDeniedException, IOException
    {
        ResponseContext context = makeCtrlConfContext(ApiOperation.makeModifyOperation());
        String logLevel = config.getLogLevel();
        String logLevelLinstor = config.getLogLevelLinstor();
        String logLevelGlobal = config.getLogLevelGlobal();
        String logLevelLinstorGlobal = config.getLogLevelLinstorGlobal();
        if (!(logLevel == null || logLevel.isEmpty()))
        {
            if (!(logLevelLinstor == null || logLevelLinstor.isEmpty()))
            {
                LinstorParsingUtils.asLogLevel(logLevel);
                LinstorParsingUtils.asLogLevel(logLevelLinstor);
                ctrlCfg.setLogLevel(logLevel);
                ctrlCfg.setLogLevelLinstor(logLevelLinstor);
                errorReporter.setLogLevel(
                    peerAccCtx.get(),
                    Level.valueOf(logLevel.toUpperCase()),
                    Level.valueOf(logLevelLinstor.toUpperCase())
                );
            }
            else
            {
                if (!(logLevelLinstorGlobal == null || logLevelLinstorGlobal.isEmpty()))
                {
                    LinstorParsingUtils.asLogLevel(logLevel);
                    LinstorParsingUtils.asLogLevel(logLevelLinstorGlobal);
                    ctrlCfg.setLogLevel(logLevel);
                    ctrlCfg.setLogLevelLinstor(logLevelLinstorGlobal);
                    errorReporter.setLogLevel(
                        peerAccCtx.get(),
                        Level.valueOf(logLevel.toUpperCase()),
                        Level.valueOf(logLevelLinstorGlobal.toUpperCase())
                    );
                }
                else
                {
                    LinstorParsingUtils.asLogLevel(logLevel);
                    ctrlCfg.setLogLevel(logLevel);
                    errorReporter
                        .setLogLevel(peerAccCtx.get(), Level.valueOf(logLevel.toUpperCase()), null);
                }
            }
        }
        else
        {
            if (!(logLevelGlobal == null || logLevelGlobal.isEmpty()))
            {
                if (!(logLevelLinstor == null || logLevelLinstor.isEmpty()))
                {
                    LinstorParsingUtils.asLogLevel(logLevelGlobal);
                    LinstorParsingUtils.asLogLevel(logLevelLinstor);
                    ctrlCfg.setLogLevel(logLevelGlobal);
                    ctrlCfg.setLogLevelLinstor(logLevelLinstor);
                    errorReporter
                        .setLogLevel(
                            peerAccCtx.get(),
                            Level.valueOf(logLevelGlobal.toUpperCase()),
                            Level.valueOf(logLevelLinstor.toUpperCase())
                        );
                }
                else
                {
                    if (!(logLevelLinstorGlobal == null || logLevelLinstorGlobal.isEmpty()))
                    {
                        LinstorParsingUtils.asLogLevel(logLevelGlobal);
                        LinstorParsingUtils.asLogLevel(logLevelLinstorGlobal);
                        ctrlCfg.setLogLevel(logLevelGlobal);
                        ctrlCfg.setLogLevelLinstor(logLevelLinstorGlobal);
                        errorReporter
                            .setLogLevel(
                                peerAccCtx.get(),
                                Level.valueOf(logLevelGlobal.toUpperCase()),
                                Level.valueOf(logLevelLinstorGlobal.toUpperCase())
                            );
                    }
                    else
                    {
                        LinstorParsingUtils.asLogLevel(logLevelGlobal);
                        ctrlCfg.setLogLevel(logLevelGlobal);
                        errorReporter.setLogLevel(
                            peerAccCtx.get(), Level.valueOf(logLevelGlobal.toUpperCase()), null
                        );
                    }
                }
            }
            else
            {
                if (!(logLevelLinstor == null || logLevelLinstor.isEmpty()))
                {
                    LinstorParsingUtils.asLogLevel(logLevelLinstor);
                    ctrlCfg.setLogLevelLinstor(logLevelLinstor);
                    errorReporter
                        .setLogLevel(peerAccCtx.get(), null, Level.valueOf(logLevelLinstor.toUpperCase()));
                }
                else
                {
                    if (!(logLevelLinstorGlobal == null || logLevelLinstorGlobal.isEmpty()))
                    {
                        LinstorParsingUtils.asLogLevel(logLevelLinstorGlobal);
                        ctrlCfg.setLogLevelLinstor(logLevelLinstorGlobal);
                        errorReporter.setLogLevel(
                            peerAccCtx.get(), null, Level.valueOf(logLevelLinstorGlobal.toUpperCase())
                        );
                    }
                }
            }
        }

        Flux<ApiCallRc> flux;
        if ((logLevelGlobal == null || logLevelGlobal.isEmpty()) &&
            (logLevelLinstorGlobal == null || logLevelLinstorGlobal.isEmpty()))
        {
            ApiCallRc rc = ApiCallRcImpl.singleApiCallRc(
                ApiConsts.MODIFIED | ApiConsts.MASK_CTRL_CONF,
                "Successfully updated controller config"
            );
            flux = Flux.just(rc);
        }
        else
        {
            SatelliteConfig stltConf = new SatelliteConfig();
            stltConf.log = new JsonGenTypes.SatelliteConfigLog();
            stltConf.log.level = logLevelGlobal;
            stltConf.log.level_linstor = logLevelLinstorGlobal;
            flux = ctrlNodeApiCallHandler.setGlobalConfig(new SatelliteConfigPojo(stltConf));
        }
        return flux.transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private static class SatelliteConfigPojo implements SatelliteConfigApi
    {
        private final SatelliteConfig config;

        SatelliteConfigPojo(SatelliteConfig configRef)
        {
            config = configRef;
        }

        @Override
        public @Nullable String getLogLevel()
        {
            return config.log.level;
        }

        @Override
        public @Nullable String getLogLevelLinstor()
        {
            return config.log.level_linstor;
        }
    }

    public static ResponseContext makeCtrlConfContext(
        ApiOperation operation
    )
    {
        Map<String, String> objRefs = new TreeMap<>();

        return new ResponseContext(
            operation,
            "Controller",
            "controller",
            ApiConsts.MASK_CTRL_CONF,
            objRefs
        );
    }

    private PairNonNull<ApiCallRc, Boolean> deleteNamespace(
        String deleteNamespaceRef,
        Map<String, PropertyChangedListener> propsChangedListenersRef
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        boolean notifyStlts = false;
        try
        {
            @Nullable Props optNamespace = systemConfRepository.getCtrlConfForChange(peerAccCtx.get())
                .getNamespace(deleteNamespaceRef);
            if (optNamespace != null)
            {
                Iterator<String> keysIterator = optNamespace.keysIterator();
                while (keysIterator.hasNext())
                {
                    TripleNonNull<ApiCallRc, Boolean, Set<Resource>> result = deleteProp(
                        keysIterator.next(),
                        deleteNamespaceRef,
                        propsChangedListenersRef
                    );
                    apiCallRc.addEntries(result.objA);
                    notifyStlts |= result.objB;
                }

                Iterator<String> iterateNamespaces = optNamespace.iterateNamespaces();
                while (iterateNamespaces.hasNext())
                {
                    PairNonNull<ApiCallRc, Boolean> result = deleteNamespace(
                        deleteNamespaceRef + "/" + iterateNamespaces.next(),
                        propsChangedListenersRef
                    );
                    apiCallRc.addEntries(result.objA);
                    notifyStlts |= result.objB;
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            String errorMsg = ResponseUtils.getAccDeniedMsg(
                peerAccCtx.get(),
                "set a controller config property"
            );
            apiCallRc.addEntry(
                errorMsg,
                ApiConsts.FAIL_ACC_DENIED_CTRL_CFG | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT
            );
            errorReporter.reportError(
                exc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return new PairNonNull<>(apiCallRc, notifyStlts);
    }

    private boolean setCtrlProp(
        AccessContext accCtx,
        String key,
        String value,
        @Nullable String namespace,
        PropertyChangedListener propChangedListenerRef
    )
        throws InvalidValueException, AccessDeniedException, DatabaseException, InvalidKeyException
    {
        String oldVal = systemConfRepository.setCtrlProp(accCtx, key, value, namespace);
        if (propChangedListenerRef != null)
        {
            propChangedListenerRef.changed(key, value, oldVal);
        }
        boolean changed;
        if (oldVal != null)
        {
            changed = !oldVal.equals(value);
        }
        else
        {
            changed = true;
        }
        return changed;
    }

    private boolean setStltProp(
        AccessContext accCtx,
        String key,
        String value,
        PropertyChangedListener propChangedListenerRef
    )
        throws InvalidValueException, AccessDeniedException, DatabaseException, InvalidKeyException
    {
        String oldVal = systemConfRepository.setStltProp(accCtx, key, value);
        if (propChangedListenerRef != null)
        {
            propChangedListenerRef.changed(key, value, oldVal);
        }
        boolean changed;
        if (oldVal != null)
        {
            changed = !oldVal.equals(value);
        }
        else
        {
            changed = true;
        }
        return changed;
    }

    /**
     * Trigger update auto-verify-algorithm for all resource definitions.
     * This is called on global enable/disable operations.
     * @return Pair of ApiCallRc and resources that got updated.
     */
    private PairNonNull<ApiCallRc, Set<Resource>> updateRscDfnsVerifyAlgo()
    {
        final Set<Resource> touchedResources = new HashSet<>();
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        for (ResourceDefinition rscDfn : rscDfnMap.values())
        {
            PairNonNull<ApiCallRc, Set<Resource>> result = ctrlRscDfnAutoVerifyAlgoHelper.updateVerifyAlgorithm(rscDfn);
            apiCallRc.addEntries(result.objA);
            touchedResources.addAll(result.objB);
        }
        return new PairNonNull<>(apiCallRc, touchedResources);
    }

    private void updateBalanceResourcesTaskSchedule(String newValue)
    {
        try
        {
            long newDelay = Long.parseLong(newValue);
            taskScheduleService.rescheduleAt(balanceResourcesTask, newDelay * 1000L);
        }
        catch (NumberFormatException nfe)
        {
            errorReporter.logError("%s property number format exception, keeping old value",
                ApiConsts.KEY_BALANCE_RESOURCES_INTERVAL);
            errorReporter.reportError(nfe);
        }
    }

    public TripleNonNull<ApiCallRc, Boolean, Set<Resource>> setProp(
        String key,
        @Nullable String namespace,
        String value,
        Map<String, PropertyChangedListener> propsChangedListenersRef
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        Set<Resource> changedRscs = new HashSet<>();
        boolean notifyStlts = false;
        try
        {
            String fullKey;
            if (namespace != null && !"".equals(namespace.trim()))
            {
                fullKey = namespace + "/" + key;
            }
            else
            {
                fullKey = key;
            }
            List<String> ignoredKeys = new ArrayList<>();
            ignoredKeys.add(ApiConsts.NAMESPC_AUXILIARY + "/");
            ignoredKeys.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

            if (fullKey.startsWith(ApiConsts.NAMESPC_CLUSTER_REMOTE))
            {
                /*
                 * special rules for "Cluster/Remote" namespace: key must be UUID and value must be unique
                 */
                handleClusterRemoteNamespace(apiCallRc, fullKey, value);
            }
            else
            if (whitelistProps.isAllowed(LinStorObject.CTRL, ignoredKeys, fullKey, value, false))
            {
                String normalized = whitelistProps.normalize(LinStorObject.CTRL, fullKey, value);

                PropertyChangedListener propChangedListener = propsChangedListenersRef.get(fullKey);
                if (fullKey.startsWith(ApiConsts.NAMESPC_REST + '/') ||
                    fullKey.startsWith(ApiConsts.NAMESPC_AUTOPLACER + "/"))
                {
                    notifyStlts = setCtrlProp(peerAccCtx.get(), key, normalized, namespace, propChangedListener);
                }
                else
                {
                    switch (fullKey)
                    {
                        case ApiConsts.KEY_TCP_PORT_AUTO_RANGE:
                            setTcpPort(key, namespace, normalized, null, apiCallRc, propChangedListener);
                            break;
                        case ApiConsts.KEY_MINOR_NR_AUTO_RANGE:
                            setMinorNr(key, namespace, normalized, apiCallRc, propChangedListener);
                            break;
                        case ApiConsts.NAMESPC_SNAPSHOT_SHIPPING + "/" + ApiConsts.KEY_TCP_PORT_RANGE:
                            setTcpPort(key, namespace, normalized, backupShipPortPool, apiCallRc, propChangedListener);
                            break;
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER:
                            notifyStlts = setCtrlProp(
                                peerAccCtx.get(),
                                key,
                                normalized,
                                namespace,
                                propChangedListener
                            );
                            break;
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_QUORUM:
                            apiCallRc.add(ApiCallRcImpl.simpleEntry(ApiConsts.WARN_DEPRECATED,
                                fullKey + " is deprecated, please use " +
                                    ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS + "/" + InternalApiConsts.KEY_DRBD_QUORUM));
                            break;
                        case ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS + "/" + InternalApiConsts.KEY_DRBD_QUORUM:
                            setCtrlProp(
                                peerAccCtx.get(),
                                ApiConsts.KEY_QUORUM_SET_BY,
                                "user",
                                ApiConsts.NAMESPC_INTERNAL_DRBD,
                                propChangedListener
                            );

                            notifyStlts = setCtrlProp(
                                peerAccCtx.get(),
                                key,
                                normalized,
                                namespace,
                                propChangedListener
                            );
                            break;
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_VERIFY_ALGO_ALLOWED_USER:
                            notifyStlts = setCtrlProp(
                                peerAccCtx.get(),
                                key,
                                normalized,
                                namespace,
                                propChangedListener
                            );
                            updateRscDfnsVerifyAlgo();
                            break;
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_DISABLE_AUTO_RESYNC_AFTER:
                        {
                            setCtrlProp(peerAccCtx.get(), key, normalized, namespace, propChangedListener);
                            PairNonNull<ApiCallRc, Set<Resource>> result;
                            if (normalized.equalsIgnoreCase("true"))
                            {
                                result = ctrlResyncAfterHelper.clearAllResyncAfterProps();
                            }
                            else
                            {
                                result = ctrlResyncAfterHelper.manage();
                            }
                            apiCallRc.addEntries(result.objA);
                            changedRscs.addAll(result.objB);
                            notifyStlts = true;
                        }
                            break;
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_DISABLE_AUTO_VERIFY_ALGO:
                        {
                            setCtrlProp(peerAccCtx.get(), key, normalized, namespace, propChangedListener);
                            // also set on satellite, so conffile builder can ignore if disabled
                            setStltProp(peerAccCtx.get(), fullKey, normalized, propChangedListener);
                            PairNonNull<ApiCallRc, Set<Resource>> result = updateRscDfnsVerifyAlgo();
                            apiCallRc.addEntries(result.objA);
                            // ignore touched resources, as we disable auto-verify-algo by conf file builder global prop
                            notifyStlts = true;
                        }
                            break;
                        case ApiConsts.KEY_BALANCE_RESOURCES_INTERVAL:
                        {
                            updateBalanceResourcesTaskSchedule(normalized);
                            setCtrlProp(peerAccCtx.get(), key, normalized, namespace, propChangedListener);
                        }
                        break;
                        case ApiConsts.KEY_AUTOPLACE_ALLOW_TARGET: // fall-through
                        case ApiConsts.KEY_SEARCH_DOMAIN: // fall-through
                        case ApiConsts.KEY_STOR_POOL_MAX_FREE_CAPACITY_OVERSUBSCRIPTION_RATIO: // fall-through
                        case ApiConsts.KEY_STOR_POOL_MAX_OVERSUBSCRIPTION_RATIO: // fall-through
                        case ApiConsts.KEY_STOR_POOL_MAX_TOTAL_CAPACITY_OVERSUBSCRIPTION_RATIO: // fall-through
                        case ApiConsts.KEY_UPDATE_CACHE_INTERVAL:
                            // fall-through
                        case ApiConsts.KEY_BALANCE_RESOURCES_ENABLED: // fall-through
                        case ApiConsts.KEY_BALANCE_RESOURCES_GRACE_PERIOD:
                            // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_AUTO_EVICT_ALLOW_EVICTION:
                            // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_AUTO_EVICT_AFTER_TIME:
                            // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_AUTO_EVICT_MAX_DISCONNECTED_NODES:
                            // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_AUTO_EVICT_MIN_REPLICA_COUNT:
                            // fall-through
                        case ApiConsts.NAMESPC_BACKUP_SHIPPING + ReadOnlyProps.PATH_SEPARATOR +
                            ApiConsts.KEY_ALLOW_FORCE_RESTORE:
                            // fall-through
                        case BackupConsts.CONCURRENT_BACKUPS_KEY:
                            // fall-through
                        case ApiConsts.KEY_RSC_ALLOW_MIXING_DEVICE_KIND:
                            // fall-through
                        case ZfsRollbackStrategy.FULL_KEY_USE_ZFS_ROLLBACK_PROP:
                            // fall-through
                        case ZfsDeleteStrategy.FULL_KEY_ZFS_DELETE_STRATEGY:
                            // fall-through
                        case ApiConsts.NAMESPC_DRBD_PROXY + "/" + ApiConsts.KEY_DRBD_PROXY_AUTO_ENABLE:
                            // no need to update stlts
                            setCtrlProp(peerAccCtx.get(), key, normalized, namespace, propChangedListener);
                            break;
                        case ApiConsts.KEY_EXT_CMD_WAIT_TO:
                            try
                            {
                                long timeout = Long.parseLong(value);
                                if (timeout < 0)
                                {
                                    throw new ApiRcException(
                                        ApiCallRcImpl.simpleEntry(
                                            ApiConsts.FAIL_INVLD_PROP,
                                            "The " + ApiConsts.KEY_EXT_CMD_WAIT_TO + " must not be negative"
                                        )
                                    );
                                }
                                ChildProcessHandler.dfltWaitTimeout = timeout;
                            }
                            catch (NumberFormatException exc)
                            {
                                throw new ApiRcException(
                                    ApiCallRcImpl.simpleEntry(
                                        ApiConsts.FAIL_INVLD_PROP,
                                        "The " + ApiConsts.KEY_EXT_CMD_WAIT_TO + " has to have a numeric value"
                                    ),
                                    exc
                                );
                            }
                            // fall-through
                        default:
                            notifyStlts = setStltProp(peerAccCtx.get(), fullKey, normalized, propChangedListener);
                            break;
                    }
                }

                apiCallRc.addEntry(
                    "Successfully set property '" + fullKey + "' to value '" + normalized + "'",
                    ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT | ApiConsts.CREATED
                );
            }
            else
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                if (whitelistProps.isKeyKnown(LinStorObject.CTRL, fullKey))
                {
                    entry.setMessage("The value '" + value + "' is not valid.");
                    entry.setDetails(
                        whitelistProps.getErrMsg(LinStorObject.CTRL, fullKey)
                    );
                }
                else
                {
                    entry.setMessage("The key '" + fullKey + "' is not whitelisted");
                }
                entry.setReturnCode(ApiConsts.FAIL_INVLD_PROP | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT);
                entry.setSkipErrorReport(true);
                apiCallRc.addEntry(entry);
            }
        }
        catch (Exception exc)
        {
            String errorMsg;
            long rc;
            boolean createErrorReport = false;
            if (exc instanceof AccessDeniedException)
            {
                errorMsg = ResponseUtils.getAccDeniedMsg(
                    peerAccCtx.get(),
                    "set a controller config property"
                );
                rc = ApiConsts.FAIL_ACC_DENIED_CTRL_CFG;
            }
            else
            if (exc instanceof InvalidKeyException)
            {
                errorMsg = "Invalid key: " + ((InvalidKeyException) exc).invalidKey;
                rc = ApiConsts.FAIL_INVLD_PROP;
            }
            else
            if (exc instanceof InvalidValueException)
            {
                errorMsg = "Invalid value: " + value;
                rc = ApiConsts.FAIL_INVLD_PROP;
            }
            else
            if (exc instanceof DatabaseException)
            {
                errorMsg = ResponseUtils.getSqlMsg(
                    "Persisting controller config prop with key '" + key + "' in namespace '" + namespace +
                    "' with value '" + value + "'."
                );
                rc = ApiConsts.FAIL_SQL;
                createErrorReport = true;
            }
            else
            {
                errorMsg = "An exception of type " + exc.getClass().getSimpleName() +
                    " occurred while setting controller config prop with key '" +
                    key + "' in namespace '" + namespace + "' with value '" + value + "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
                createErrorReport = true;
            }

            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(
                rc | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT, errorMsg, !createErrorReport));
            if (createErrorReport)
            {
                errorReporter.reportError(
                    exc,
                    peerAccCtx.get(),
                    null,
                    errorMsg
                );
            }
        }
        return new TripleNonNull<>(apiCallRc, notifyStlts, changedRscs);
    }

    private void handleClusterRemoteNamespace(ApiCallRcImpl apiCallRc, String fullKey, String value)
        throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException
    {
        ApiCallRcEntry entry = null;

        // +1 for the trailing "/"
        String actualKey = fullKey.substring(ApiConsts.NAMESPC_CLUSTER_REMOTE.length() + 1);
        if (UuidUtils.isUuid(actualKey))
        {
            if (systemConfRepository.getCtrlConfForView(peerAccCtx.get()).values().contains(value))
            {
                entry = new ApiCallRcEntry();
                entry.setMessage("The value '" + value + "' is already used for another remote cluster");
            }
            else if (value.length() > MAX_REMOTE_NAME_LEN)
            {
                entry = new ApiCallRcEntry();
                entry.setMessage("The value '" + value + "' is longer than " + MAX_REMOTE_NAME_LEN);
            }
        }
        else
        {
            entry = new ApiCallRcEntry();
            entry.setMessage("The key '" + actualKey + "' must be a valid UUID");
        }
        if (entry != null)
        {
            entry.setReturnCode(ApiConsts.FAIL_INVLD_PROP | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT);
            apiCallRc.addEntry(entry);
        }
        else
        {
            systemConfRepository.getCtrlConfForChange(peerAccCtx.get()).setProp(
                actualKey,
                value,
                ApiConsts.NAMESPC_CLUSTER_REMOTE
            );
            // no need to update satellites
            transMgrProvider.get().commit();

            apiCallRc.addEntry(
                "Successfully set property '" + fullKey + "' to value '" + value + "'",
                ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT | ApiConsts.CREATED
            );

        }
    }

    private ApiCallRc handleNetComModifications(
        HashMap<String, String> filteredOverrideProps,
        HashSet<String> filteredDeletePropKeys,
        HashSet<String> filteredDeleteNamespaces,
        Map<String, PropertyChangedListener> propsChangedListenersRef
    )
    {
        // just apply all netcom props and perform sanity checks later
        String currentKey = null;
        String currentValue = null;
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            HashSet<String> connectorsToCheck = new HashSet<>();
            Iterator<Entry<String, String>> overrideIterator = filteredOverrideProps.entrySet().iterator();
            while (overrideIterator.hasNext())
            {
                Entry<String, String> overrideProp = overrideIterator.next();
                currentKey = overrideProp.getKey();
                currentValue = overrideProp.getValue();
                String[] splitByNamespaces = currentKey.split("/");
                if (currentKey.startsWith(ApiConsts.NAMESPC_NETCOM) && splitByNamespaces.length == 3)
                {
                    String normalized = whitelistProps.normalize(LinStorObject.CTRL, currentKey, currentValue);
                    setCtrlProp(
                        peerAccCtx.get(),
                        currentKey,
                        normalized,
                        null,
                        propsChangedListenersRef.get(currentKey)
                    );
                    connectorsToCheck.add(splitByNamespaces[1]);
                    overrideIterator.remove(); // remove the prop to not being forwarded to the default handler
                }
            }
            currentValue = null;
            Iterator<String> deletePropIterator = filteredDeletePropKeys.iterator();
            while (deletePropIterator.hasNext())
            {
                currentKey = deletePropIterator.next();
                String[] splitByNamespaces = currentKey.split("/");
                if (currentKey.startsWith(ApiConsts.NAMESPC_NETCOM) && splitByNamespaces.length == 3)
                {
                    systemConfRepository.removeCtrlProp(peerAccCtx.get(), currentKey, null);
                    connectorsToCheck.add(splitByNamespaces[1]);
                    deletePropIterator.remove(); // remove the prop to not being forwarded to the default handler
                }
            }

            Iterator<String> deleteNamespaceIterator = filteredDeleteNamespaces.iterator();
            while (deleteNamespaceIterator.hasNext())
            {
                currentKey = deleteNamespaceIterator.next();
                String[] splitByNamespaces = currentKey.split("/");
                if (currentKey.startsWith(ApiConsts.NAMESPC_NETCOM) && splitByNamespaces.length == 2)
                {
                    @Nullable Props optNamespace = systemConfRepository.getCtrlConfForChange(peerAccCtx.get())
                        .getNamespace(currentKey);
                    if (optNamespace != null)
                    {
                        Iterator<String> keysIterator = optNamespace.keysIterator();
                        while (keysIterator.hasNext())
                        {
                            String actualKey = keysIterator.next();
                            systemConfRepository.removeCtrlProp(peerAccCtx.get(), actualKey, null);
                        }
                    }
                    deleteNamespaceIterator.remove(); // remove the prop to not being forwarded to the default handler
                }
            }

            boolean abort = false;
            final String missingKeyFormat = "NetComConnector '%s' is missing '%s' key.";
            if (!connectorsToCheck.isEmpty())
            {
                @Nullable Props ctrlProps = systemConfRepository.getCtrlConfForChange(peerAccCtx.get())
                    .getNamespace(ApiConsts.NAMESPC_NETCOM);
                if (ctrlProps != null)
                {
                    for (String connectorToCheck : connectorsToCheck)
                    {
                        @Nullable Props conNamespace = ctrlProps.getNamespace(connectorToCheck);
                        if (conNamespace != null)
                        {
                            List<String> errorMessages = new ArrayList<>();

                            BiConsumer<String, String> addErrorIfNull = (value, missingKey) ->
                            {
                                if (value == null)
                                {
                                    errorMessages.add(String.format(missingKeyFormat, connectorToCheck, missingKey));
                                }
                            };

                            String enabled = conNamespace.getProp(ApiConsts.KEY_NETCOM_ENABLED);
                            String bindAddress = conNamespace.getProp(ApiConsts.KEY_NETCOM_BIND_ADDRESS);
                            String keyPasswd = conNamespace.getProp(ApiConsts.KEY_NETCOM_KEY_PASSWD);
                            String keyStore = conNamespace.getProp(ApiConsts.KEY_NETCOM_KEY_STORE);
                            String keyStorePasswd = conNamespace.getProp(ApiConsts.KEY_NETCOM_KEY_STORE_PASSWD);
                            String port = conNamespace.getProp(ApiConsts.KEY_NETCOM_PORT);
                            String sslProtocol = conNamespace.getProp(ApiConsts.KEY_NETCOM_SSL_PROTOCOL);
                            String trustStore = conNamespace.getProp(ApiConsts.KEY_NETCOM_TRUST_STORE);
                            String trustStorePasswd = conNamespace.getProp(ApiConsts.KEY_NETCOM_TRUST_STORE_PASSWD);
                            String type = conNamespace.getProp(ApiConsts.KEY_NETCOM_TYPE);

                            addErrorIfNull.accept(bindAddress, ApiConsts.KEY_NETCOM_BIND_ADDRESS);
                            addErrorIfNull.accept(type, ApiConsts.KEY_NETCOM_TYPE);
                            addErrorIfNull.accept(port, ApiConsts.KEY_NETCOM_PORT);
                            addErrorIfNull.accept(enabled, ApiConsts.KEY_NETCOM_ENABLED);

                            if (ApiConsts.VAL_NETCOM_TYPE_SSL.equalsIgnoreCase(type))
                            {
                                addErrorIfNull.accept(keyPasswd, ApiConsts.KEY_NETCOM_KEY_PASSWD);
                                addErrorIfNull.accept(keyStore, ApiConsts.KEY_NETCOM_KEY_STORE);
                                addErrorIfNull.accept(keyStorePasswd, ApiConsts.KEY_NETCOM_KEY_STORE_PASSWD);
                                addErrorIfNull.accept(sslProtocol, ApiConsts.KEY_NETCOM_SSL_PROTOCOL);
                                addErrorIfNull.accept(trustStore, ApiConsts.KEY_NETCOM_TRUST_STORE);
                                addErrorIfNull.accept(trustStorePasswd, ApiConsts.KEY_NETCOM_TRUST_STORE_PASSWD);
                            }

                            // normalize the 'enabled' property. we cannot use the whitelist normialization as we have
                            // no whitelist rule for these (special) netcom namespace
                            boolean isEnabled;
                            if (ApiConsts.VAL_TRUE.equalsIgnoreCase(enabled))
                            {
                                isEnabled = true;
                                if (!ApiConsts.VAL_TRUE.equals(enabled))
                                {
                                    conNamespace.setProp(ApiConsts.KEY_NETCOM_ENABLED, ApiConsts.VAL_TRUE);
                                }
                            }
                            else if (ApiConsts.VAL_FALSE.equalsIgnoreCase(enabled))
                            {
                                isEnabled = false;
                                if (!ApiConsts.VAL_FALSE.equals(enabled))
                                {
                                    conNamespace.setProp(ApiConsts.KEY_NETCOM_ENABLED, ApiConsts.VAL_FALSE);
                                }
                            }
                            else
                            {
                                // nothing valid
                                isEnabled = false;
                            }

                            if (!errorMessages.isEmpty())
                            {
                                apiCallRc.addEntry(
                                    ApiCallRcImpl.simpleEntry(
                                        isEnabled ? ApiConsts.FAIL_INVLD_CONF : ApiConsts.WARN_INVLD_CONF,
                                        StringUtils.join(errorMessages, "\n"),
                                        true
                                    )
                                );
                                abort = isEnabled;
                            }
                        }
                    }
                }
            }
            if (abort)
            {
                transMgrProvider.get().rollback();
                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_INVLD_CONF, "Invalid configurations were rolled back")
                );
            }
            else
            {
                transMgrProvider.get().commit();
                // TODO: restart valid but changed netCom services
            }
        }
        catch (Exception exc)
        {
            String errorMsg;
            long rc;
            if (exc instanceof AccessDeniedException)
            {
                errorMsg = ResponseUtils.getAccDeniedMsg(
                    peerAccCtx.get(),
                    "set a netcom (controller) config property"
                );
                rc = ApiConsts.FAIL_ACC_DENIED_CTRL_CFG;
            }
            else if (exc instanceof InvalidKeyException)
            {
                errorMsg = "Invalid key: " + ((InvalidKeyException) exc).invalidKey;
                rc = ApiConsts.FAIL_INVLD_PROP;
            }
            else if (exc instanceof InvalidValueException)
            {
                errorMsg = "Invalid value: " + currentValue + " for key: " + currentKey;
                rc = ApiConsts.FAIL_INVLD_PROP;
            }
            else if (exc instanceof DatabaseException)
            {
                errorMsg = ResponseUtils.getSqlMsg(
                    "Persisting controller config prop with key '" + currentKey + "' with value '" + currentValue + "'."
                );
                rc = ApiConsts.FAIL_SQL;
            }
            else
            {
                errorMsg = "An exception of type " + exc.getClass().getSimpleName() +
                    " occurred while setting netcom (controller) config prop with key '" +
                    currentKey + "' with value '" + currentValue + "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(errorMsg, rc | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT);
            errorReporter.reportError(
                exc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return apiCallRc;
    }

    public Map<String, String> listProps()
    {
        Map<String, String> mergedMap = new TreeMap<>();
        try
        {
            mergedMap.putAll(systemConfRepository.getCtrlConfForView(peerAccCtx.get()).map());
            mergedMap.putAll(systemConfRepository.getStltConfForView(peerAccCtx.get()).map());
        }
        catch (Exception ignored)
        {
            // empty list
        }
        return mergedMap;
    }

    public Flux<ApiCallRc> deletePropWithCommit(String key, String namespace)
    {
        ResponseContext context = makeCtrlConfContext(
            ApiOperation.makeDeleteOperation()
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "deletePropWithCommit",
                lockGuardFactory.buildDeferred(WRITE, LockObj.CTRL_CONFIG),
                () -> deletePropWithCommitInTransaction(key, namespace)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    public Flux<ApiCallRc> deletePropWithCommitInTransaction(String key, String namespace)
    {
        TripleNonNull<ApiCallRc, Boolean, Set<Resource>> result = deleteProp(
            key,
            namespace,
            new HashMap<>() // XXX is this method even called?
        );
        transMgrProvider.get().commit();
        if (result.objB)
        {
            try
            {
                updateSatelliteConf();
            }
            catch (AccessDeniedException exc)
            {
                throw new ApiRcException(ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.FAIL_ACC_DENIED_CTRL_CFG,
                    ResponseUtils.getAccDeniedMsg(
                        peerAccCtx.get(),
                        "delete a controller config property"
                    )));
            }
        }
        Flux<ApiCallRc> fluxUpdRscDfns = Flux.empty();
        for (Resource rsc : result.objC)
        {
            fluxUpdRscDfns = fluxUpdRscDfns.concatWith(
                ctrlSatelliteUpdateCaller.updateSatellites(rsc, Flux.empty())
                    .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2()));
        }
        return fluxUpdRscDfns.concatWith(Flux.just(result.objA));
    }

    private TripleNonNull<ApiCallRc, Boolean, Set<Resource>> deleteProp(
        String key,
        @Nullable String namespace,
        Map<String, PropertyChangedListener> propsChangedListenersRef
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        boolean notifyStlts = false;
        Set<Resource> changedRscs = new HashSet<>();
        try
        {
            String fullKey;
            if (namespace != null && !"".equals(namespace.trim()))
            {
                fullKey = namespace + "/" + key;
            }
            else
            {
                fullKey = key;
            }
            List<String> ignoredKeys = new ArrayList<>();
            ignoredKeys.add(ApiConsts.NAMESPC_AUXILIARY + "/");
            ignoredKeys.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

            boolean isPropWhitelisted = whitelistProps.isAllowed(
                LinStorObject.CTRL,
                ignoredKeys,
                fullKey,
                null,
                false
            );
            if (isPropWhitelisted)
            {
                String oldValue = systemConfRepository.removeCtrlProp(peerAccCtx.get(), key, namespace);
                notifyStlts = systemConfRepository.removeStltProp(peerAccCtx.get(), key, namespace) != null;

                if (oldValue != null)
                {
                    notifyStlts = true;
                    switch (fullKey)
                    {
                        case ApiConsts.KEY_TCP_PORT_AUTO_RANGE:
                            reloadAllNodesTcpPortPools();
                            break;
                        case ApiConsts.KEY_MINOR_NR_AUTO_RANGE:
                            minorNrPool.reloadRange();
                            break;
                        case ApiConsts.KEY_TCP_PORT_RANGE:
                            backupShipPortPool.reloadRange();
                            break;
                        case ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS + "/" + InternalApiConsts.KEY_DRBD_QUORUM:
                            systemConfRepository.removeCtrlProp(
                                peerAccCtx.get(), ApiConsts.KEY_QUORUM_SET_BY, ApiConsts.NAMESPC_INTERNAL_DRBD);
                            break;
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_DISABLE_AUTO_RESYNC_AFTER:
                        {
                            PairNonNull<ApiCallRc, Set<Resource>> result = ctrlResyncAfterHelper.manage();
                            apiCallRc.addEntries(result.objA);
                            changedRscs.addAll(result.objB);
                        }
                            break;
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_DISABLE_AUTO_VERIFY_ALGO:
                        {
                            PairNonNull<ApiCallRc, Set<Resource>> result = updateRscDfnsVerifyAlgo();
                            apiCallRc.addEntries(result.objA);
                            changedRscs.addAll(result.objB);
                        }
                            break;
                        // TODO: check for other properties
                        default:
                            // ignore - for now
                    }

                    PropertyChangedListener listener = propsChangedListenersRef.get(fullKey);
                    if (listener != null)
                    {
                        listener.changed(fullKey, null, oldValue);
                    }
                }

                apiCallRc.addEntry(
                    "Successfully deleted property '" + fullKey + "'",
                    ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_DEL | ApiConsts.DELETED
                );
            }
            else
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setMessage("The key '" + fullKey + "' is not whitelisted");
                entry.setReturnCode(ApiConsts.FAIL_INVLD_PROP | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_DEL);
                apiCallRc.addEntry(entry);
            }
        }
        catch (Exception exc)
        {
            String errorMsg;
            long rc;
            if (exc instanceof AccessDeniedException)
            {
                errorMsg = ResponseUtils.getAccDeniedMsg(
                    peerAccCtx.get(),
                    "delete a controller config property"
                );
                rc = ApiConsts.FAIL_ACC_DENIED_CTRL_CFG;
            }
            else
            if (exc instanceof InvalidKeyException)
            {
                errorMsg = "Invalid key: " + ((InvalidKeyException) exc).invalidKey;
                rc = ApiConsts.FAIL_INVLD_PROP;
            }
            else
            {
                errorMsg = "An exception of type " + exc.getClass().getSimpleName() +
                    " occurred while deleting controller config prop with key '" +
                    key + "' in namespace '" + namespace + "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(errorMsg, rc);
            errorReporter.reportError(
                exc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return new TripleNonNull<>(apiCallRc, notifyStlts, changedRscs);
    }

    private void reloadAllNodesTcpPortPools() throws AccessDeniedException
    {
        AccessContext peerCtx = peerAccCtx.get();
        for (Node node : nodesMap.values())
        {
            node.getTcpPortPool(peerCtx).reloadRange();
        }
    }

    public LinstorEncryptionStatus masterPassphraseStatus()
    {
        LinstorEncryptionStatus status = LinstorEncryptionStatus.UNSET;
        try
        {
            ReadOnlyProps namespace = encHelper.getEncryptedNamespace(peerAccCtx.get());
            if (!(namespace == null || namespace.isEmpty()))
            {
                status = encHelper.isMasterKeyUnlocked() ?
                    LinstorEncryptionStatus.UNLOCKED : LinstorEncryptionStatus.LOCKED;
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_ACC_DENIED_CTRL_CFG,
                ResponseUtils.getAccDeniedMsg(peerAccCtx.get(), "view the controller properties")
            ));
        }
        return status;
    }

    public Flux<ApiCallRc> enterPassphrase(String passphrase)
    {
        ResponseContext context = makeCtrlConfContext(
            ApiOperation.makeModifyOperation()
        );

        return scopeRunner.fluxInTransactionalScope(
            "Entering passphrase",
            lockGuardFactory.buildDeferred(WRITE, LockObj.CTRL_CONFIG),
            () -> enterPassphraseInTransaction(
                passphrase
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> enterPassphraseInTransaction(String passphrase)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            ReadOnlyProps namespace = encHelper.getEncryptedNamespace(peerAccCtx.get());
            if (namespace == null || namespace.isEmpty())
            {
                ResponseUtils.reportStatic(
                    null,
                    EncryptionHelper.NAMESPACE_ENCRYPTED + " namespace is empty, you need to set a passphrase first",
                    ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                    null,
                    true,
                    apiCallRc,
                    errorReporter,
                    peerAccCtx.get(),
                    peerProvider.get()
                );
            }
            else
            {
                byte[] decryptedMasterKey = encHelper.getDecryptedMasterKey(
                    namespace,
                    passphrase.getBytes(StandardCharsets.UTF_8)
                );
                flux = encHelper.setCryptKey(decryptedMasterKey, namespace, true);
                // setCryptKey might have changed volatileRscData (ignoreReason, etc..)
                transMgrProvider.get().commit();

                ResponseUtils.reportSuccessStatic(
                    "Passphrase accepted",
                    null,
                    ApiConsts.MASK_CTRL_CONF | ApiConsts.PASSPHRASE_ACCEPTED,
                    apiCallRc,
                    null,
                    errorReporter
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            ResponseUtils.addAnswerStatic(
                ResponseUtils.getAccDeniedMsg(peerAccCtx.get(), "view the controller properties"),
                null, // cause
                null, // details
                null, // correction
                ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_ACC_DENIED_CTRL_CFG,
                null, // objRefs
                null, // errorId
                false,
                apiCallRc
            );
        }
        catch (InvalidKeyException exc)
        {
            ResponseUtils.reportStatic(
                new ImplementationError(
                    "Hardcoded namespace or property key invalid",
                    exc
                ),
                "Hardcoded namespace or property key invalid",
                ApiConsts.FAIL_IMPL_ERROR,
                null,
                false,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        catch (MissingKeyPropertyException exc)
        {
            ResponseUtils.addAnswerStatic(
                "Could not restore crypt passphrase as one of the following properties is not set:\n" +
                    "'" + EncryptionHelper.KEY_CRYPT_HASH + "', '" + EncryptionHelper.KEY_CRYPT_KEY + "', '" +
                    EncryptionHelper.KEY_PASSPHRASE_SALT + "'",
                "This is either an implementation error or a user has manually removed one of the " +
                    "mentioned protperties.",
                null, // details
                null, // correction
                ApiConsts.MASK_MOD | ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                null, // objectRefs
                null, // errorId
                true,
                apiCallRc
            );
        }
        catch (IncorrectPassphraseException exc)
        {
            ResponseUtils.addAnswerStatic(
                "Could not restore master passphrase as the given old passphrase was incorrect",
                "Wrong passphrase", // cause
                null, // details
                "Enter the correct passphrase", // correction
                ApiConsts.MASK_MOD | ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                null, // objectRefs
                null, // errorId
                true,
                apiCallRc
            );
        }
        catch (LinStorException exc)
        {
            ResponseUtils.reportStatic(
                exc,
                "An exception of type " + exc.getClass().getSimpleName() +
                " occurred while validating the passphrase",
                ApiConsts.FAIL_UNKNOWN_ERROR,
                null,
                false,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }

        return Flux.<ApiCallRc>just(apiCallRc).concatWith(flux);
    }

    public Flux<ApiCallRc> setPassphrase(String newPassphrase, @Nullable String oldPassphrase)
    {
        ResponseContext context = makeCtrlConfContext(
            ApiOperation.makeCreateOperation()
        );

        return scopeRunner.fluxInTransactionalScope(
            (oldPassphrase == null ? "Creating" : "Modifying") + " passphrase",
            lockGuardFactory.buildDeferred(WRITE, LockObj.CTRL_CONFIG),
            () -> setPassphraseInTransaction(
                newPassphrase,
                oldPassphrase
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> setPassphraseInTransaction(String newPassphrase, @Nullable String oldPassphrase)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        long mask = ApiConsts.MASK_CTRL_CONF;
        try
        {
            ReadOnlyProps namespace = encHelper.getEncryptedNamespace(peerAccCtx.get());

            if (oldPassphrase == null)
            {
                mask |= ApiConsts.MASK_CRT;
                if (namespace == null || namespace.getProp(EncryptionHelper.KEY_CRYPT_KEY) == null)
                {
                    // no oldPassphrase and empty namespace means that
                    // this is the initial passphrase
                    byte[] masterKey = encHelper.generateSecret();
                    encHelper.setPassphraseImpl(
                        newPassphrase.getBytes(StandardCharsets.UTF_8),
                        masterKey,
                        peerAccCtx.get()
                    );
                    // setPassphraseImpl sets the props in this namespace; to ensure they are there, get it again
                    namespace = encHelper.getEncryptedNamespace(peerAccCtx.get());
                    flux = encHelper.setCryptKey(masterKey, namespace, true);

                    // setCryptKey could have changed voaltileRscData (ignoreReasons, etc...)
                    transMgrProvider.get().commit();

                    ResponseUtils.reportSuccessStatic(
                         "Crypt passphrase created.",
                         null, // details
                         mask | ApiConsts.CREATED,
                         apiCallRc,
                         null, // objectRefs
                         errorReporter
                    );
                }
                else
                {
                    ResponseUtils.addAnswerStatic(
                        "Could not create new crypt passphrase as it already exists",
                        "A passphrase was already defined",
                        null,
                        "Use the crypt-modify-passphrase command instead of crypt-create-passphrase",
                        mask | ApiConsts.FAIL_EXISTS_CRYPT_PASSPHRASE,
                        new HashMap<>(),
                        null, // errorId
                        true,
                        apiCallRc
                    );
                }
            }
            else
            {
                mask |= ApiConsts.MASK_MOD;
                if (namespace == null || namespace.isEmpty())
                {
                    ResponseUtils.addAnswerStatic(
                        "Could not modify crypt passphrase as it does not exist",
                        "No passphrase was defined yet",
                        null,
                        "Use the crypt-create-passphrase command instead of crypt-modify-passphrase",
                        mask | ApiConsts.FAIL_EXISTS_CRYPT_PASSPHRASE,
                        new HashMap<>(),
                        null, // errorId
                        true,
                        apiCallRc
                    );
                }
                else
                {
                    byte[] decryptedMasterKey = encHelper.getDecryptedMasterKey(
                        namespace,
                        oldPassphrase.getBytes(StandardCharsets.UTF_8)
                    );
                    encHelper.setPassphraseImpl(
                        newPassphrase.getBytes(StandardCharsets.UTF_8),
                        decryptedMasterKey,
                        peerAccCtx.get()
                    );
                    ResponseUtils.reportSuccessStatic(
                        "Crypt passphrase updated",
                        null, // details
                        mask | ApiConsts.MODIFIED,
                        apiCallRc,
                        null, // objectRefs
                        errorReporter
                    );
                }
            }

        }
        catch (InvalidKeyException invalidNameExc)
        {
            ResponseUtils.reportStatic(
                new ImplementationError(
                    "Hardcoded namespace or property key invalid",
                    invalidNameExc
                ),
                "Hardcoded namespace or property key invalid",
                ApiConsts.FAIL_IMPL_ERROR,
                null,
                false,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            ResponseUtils.reportStatic(
                accDeniedExc,
                ResponseUtils.getAccDeniedMsg(peerAccCtx.get(), "access the controller properties"),
                ApiConsts.FAIL_ACC_DENIED_CTRL_CFG,
                null, // objects
                false,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        catch (InvalidValueException exc)
        {
            ResponseUtils.reportStatic(
                new ImplementationError(exc),
                "Generated key could not be stored as property",
                ApiConsts.FAIL_IMPL_ERROR,
                null,
                false,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        catch (DatabaseException exc)
        {
            ResponseUtils.reportStatic(
                exc,
                ResponseUtils.getSqlMsg("storing the generated and encrypted master key"),
                ApiConsts.FAIL_SQL,
                null,
                false,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        catch (MissingKeyPropertyException exc)
        {
            ResponseUtils.addAnswerStatic(
                "Could not restore crypt passphrase as one of the following properties is not set:\n" +
                    "'" + EncryptionHelper.KEY_CRYPT_HASH + "', '" + EncryptionHelper.KEY_CRYPT_KEY + "', '" +
                    EncryptionHelper.KEY_PASSPHRASE_SALT + "'",
                "This is either an implementation error or a user has manually removed one of the " +
                    "mentioned protperties.",
                null, // details
                null, // correction
                ApiConsts.MASK_MOD | ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                null, // objectRefs
                null, // errorId
                false,
                apiCallRc
            );
        }
        catch (IncorrectPassphraseException exc)
        {
            ResponseUtils.addAnswerStatic(
                "Could not restore master passphrase as the given old passphrase was incorrect",
                "Wrong passphrase", // cause
                null, // details
                "Enter the correct passphrase", // correction
                ApiConsts.MASK_MOD | ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                null, // objectRefs
                null, // errorId
                true,
                apiCallRc
            );
        }
        catch (LinStorException exc)
        {
            ResponseUtils.reportStatic(
                exc,
                "An exception of type " + exc.getClass().getSimpleName() + " occurred while setting the passphrase",
                ApiConsts.FAIL_UNKNOWN_ERROR,
                null,
                false,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        return Flux.<ApiCallRc>just(apiCallRc)
            .concatWith(flux);
    }

    private void setTcpPort(
        String key,
        String namespace,
        String value,
        @Nullable DynamicNumberPool poolToReloadRef,
        ApiCallRcImpl apiCallRc,
        PropertyChangedListener propChangedListenerRef
    )
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, DatabaseException
    {
        Matcher matcher = NumberPoolModule.RANGE_PATTERN.matcher(value);
        if (matcher.find())
        {
            if (
                isValidTcpPort(matcher.group("min"), apiCallRc) &&
                isValidTcpPort(matcher.group("max"), apiCallRc)
            )
            {
                @Nullable String oldValue = systemConfRepository.setCtrlProp(peerAccCtx.get(), key, value, namespace);
                if (poolToReloadRef != null)
                {
                    poolToReloadRef.reloadRange();
                }
                else
                {
                    reloadAllNodesTcpPortPools();
                }

                if (propChangedListenerRef != null)
                {
                    propChangedListenerRef.changed(key, value, oldValue);
                }

                apiCallRc.addEntry(
                    "The TCP port range was successfully updated to: " + value,
                    ApiConsts.MODIFIED
                );
            }
        }
        else
        {
            String errMsg = "The given value '" + value + "' is not a valid range. '" +
                NumberPoolModule.RANGE_PATTERN.pattern() + "'";
            apiCallRc.addEntry(
                errMsg,
                ApiConsts.FAIL_INVLD_TCP_PORT
            );
        }
    }

    private boolean isValidTcpPort(String strTcpPort, ApiCallRcImpl apiCallRc)
    {
        boolean validTcpPortNr = false;
        try
        {
            TcpPortNumber.tcpPortNrCheck(Integer.parseInt(strTcpPort));
            validTcpPortNr = true;
        }
        catch (NumberFormatException | ValueOutOfRangeException exc)
        {
            String errorMsg;
            long rc;
            if (exc instanceof NumberFormatException)
            {
                errorMsg = "The given tcp port number is not a valid integer: '" + strTcpPort + "'.";
            }
            else
            {
                errorMsg = "The given tcp port number is not valid: '" + strTcpPort + "'.";
            }
            rc = ApiConsts.FAIL_INVLD_TCP_PORT;

            apiCallRc.addEntry(
                errorMsg,
                rc
            );
            errorReporter.reportError(
                exc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return validTcpPortNr;
    }


    private void setMinorNr(
        String key,
        String namespace,
        String value,
        ApiCallRcImpl apiCallRc,
        PropertyChangedListener propChangedListenerRef
    )
        throws AccessDeniedException, InvalidKeyException, InvalidValueException, DatabaseException
    {
        Matcher matcher = NumberPoolModule.RANGE_PATTERN.matcher(value);
        if (matcher.find())
        {
            if (
                isValidMinorNr(matcher.group("min"), apiCallRc) &&
                isValidMinorNr(matcher.group("max"), apiCallRc)
            )
            {
                String oldValue = systemConfRepository.setCtrlProp(peerAccCtx.get(), key, value, namespace);
                minorNrPool.reloadRange();

                if (propChangedListenerRef != null)
                {
                    propChangedListenerRef.changed(key, value, oldValue);
                }

                apiCallRc.addEntry(
                    "The Minor range was successfully updated to: " + value,
                    ApiConsts.MODIFIED
                );
            }
        }
        else
        {
            String errMsg = "The given value '" + value + "' is not a valid range. '" +
                NumberPoolModule.RANGE_PATTERN.pattern() + "'";
            apiCallRc.addEntry(
                errMsg,
                ApiConsts.FAIL_INVLD_MINOR_NR
            );
        }
    }

    private boolean isValidMinorNr(String strMinorNr, ApiCallRcImpl apiCallRc)
    {
        boolean isValid = false;
        try
        {
            MinorNumber.minorNrCheck(Integer.parseInt(strMinorNr));
            isValid = true;
        }
        catch (NumberFormatException | ValueOutOfRangeException exc)
        {
            String errorMsg;
            long rc;
            if (exc instanceof NumberFormatException)
            {
                errorMsg = "The given minor number is not a valid integer: '" + strMinorNr + "'.";
            }
            else
            {
                errorMsg = "The given minor number is not valid: '" + strMinorNr + "'.";
            }
            rc = ApiConsts.FAIL_INVLD_MINOR_NR;

            apiCallRc.addEntry(
                errorMsg,
                rc
            );
            errorReporter.reportError(
                exc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return isValid;
    }
}
