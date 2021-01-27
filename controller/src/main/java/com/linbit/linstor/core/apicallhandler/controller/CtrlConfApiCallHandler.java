package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.extproc.ChildProcessHandler;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.SatelliteConfig;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IncorrectPassphraseException;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.MissingKeyPropertyException;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.apis.ControllerConfigApi;
import com.linbit.linstor.core.apis.SatelliteConfigApi;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.objects.Node;
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
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.AutoDiskfulTask;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;

import com.google.inject.Provider;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlConfApiCallHandler
{

    private final ErrorReporter errorReporter;
    private final SystemConfRepository systemConfRepository;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorNrPool;
    private final DynamicNumberPool snapShipPortPool;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<Peer> peerProvider;
    private final Provider<TransactionMgr> transMgrProvider;

    private final CtrlStltSerializer ctrlStltSrzl;
    private final NodesMap nodesMap;
    private final WhitelistProps whitelistProps;
    private final EncryptionHelper encHelper;
    private final CtrlConfig ctrlCfg;
    private final ScopeRunner scopeRunner;
    private final ResponseConverter responseConverter;
    private final CtrlNodeApiCallHandler ctrlNodeApiCallHandler;

    private final LockGuardFactory lockGuardFactory;
    private AutoDiskfulTask autoDiskfulTask;


    @Inject
    public CtrlConfApiCallHandler(
        ErrorReporter errorReporterRef,
        SystemConfRepository systemConfRepositoryRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorNrPoolRef,
        @Named(
            NumberPoolModule.SNAPSHOPT_SHIPPING_PORT_POOL
        ) DynamicNumberPool snapShipPortPoolRef,
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
        AutoDiskfulTask autoDiskfulTaskRef
    )
    {
        errorReporter = errorReporterRef;
        systemConfRepository = systemConfRepositoryRef;
        tcpPortPool = tcpPortPoolRef;
        minorNrPool = minorNrPoolRef;
        snapShipPortPool = snapShipPortPoolRef;
        peerAccCtx = peerAccCtxRef;
        peerProvider = peerProviderRef;
        transMgrProvider = transMgrProviderRef;

        nodesMap = nodesMapRef;
        ctrlStltSrzl = ctrlStltSrzlRef;
        whitelistProps = whitelistPropsRef;
        encHelper = encHelperRef;
        lockGuardFactory = lockGuardFactoryRef;
        scopeRunner = scopeRunnerRef;
        ctrlCfg = ctrlCfgRef;
        responseConverter = responseConverterRef;
        ctrlNodeApiCallHandler = ctrlNodeApiCallHandlerRef;
        autoDiskfulTask = autoDiskfulTaskRef;
    }

    private void updateSatelliteConf() throws AccessDeniedException
    {
        for (Node nodeToContact : nodesMap.values())
        {
            Peer satellitePeer = nodeToContact.getPeer(peerAccCtx.get());

            if (satellitePeer.isConnected() && !satellitePeer.hasFullSyncFailed())
            {
                byte[] changedMessage = ctrlStltSrzl
                    .onewayBuilder(InternalApiConsts.API_CHANGED_CONTROLLER)
                    .build();

                satellitePeer.sendMessage(changedMessage);
            }
        }
    }

    public ApiCallRc modifyCtrl(
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespacesRef
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        for (Entry<String, String> overrideProp : overridePropsRef.entrySet())
        {
            apiCallRc.addEntries(setProp(overrideProp.getKey(), null, overrideProp.getValue()));
        }
        for (String deletePropKey : deletePropKeysRef)
        {
            apiCallRc.addEntries(deleteProp(deletePropKey, null));
        }
        for (String deleteNamespace : deletePropNamespacesRef)
        {
            // we should not simply "drop" the namespace here, as we might have special cleanup logic
            // for some of the deleted keys.
            apiCallRc.addEntries(deleteNamespace(deleteNamespace));
        }

        String autoDiskfulKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_DISKFUL;
        if (
            overridePropsRef.containsKey(autoDiskfulKey) || deletePropKeysRef.contains(autoDiskfulKey) ||
                deletePropNamespacesRef.contains(ApiConsts.NAMESPC_DRBD_OPTIONS)
        )
        {
            autoDiskfulTask.update();
        }

        return apiCallRc;
    }

    public Flux<ApiCallRc> setCtrlConfig(
        ControllerConfigApi config
    )
        throws AccessDeniedException
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
                    peerAccCtx.get(), Level.valueOf(logLevel.toUpperCase()), Level.valueOf(logLevelLinstor.toUpperCase())
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
                        peerAccCtx.get(), Level.valueOf(logLevel.toUpperCase()), Level.valueOf(logLevelLinstorGlobal.toUpperCase())
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
                        .setLogLevel(peerAccCtx.get(), Level.valueOf(logLevelGlobal.toUpperCase()), Level.valueOf(logLevelLinstor.toUpperCase()));
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
                            .setLogLevel(peerAccCtx.get(), Level.valueOf(logLevelGlobal.toUpperCase()), Level.valueOf(logLevelLinstorGlobal.toUpperCase()));
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

    private class SatelliteConfigPojo implements SatelliteConfigApi
    {
        private final SatelliteConfig config;

        SatelliteConfigPojo(SatelliteConfig configRef)
        {
            config = configRef;
        }

        @Override
        public String getLogLevel()
        {
            return config.log.level;
        }

        @Override
        public String getLogLevelLinstor()
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

    private ApiCallRcImpl deleteNamespace(String deleteNamespaceRef)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            Optional<Props> optNamespace = systemConfRepository.getCtrlConfForChange(peerAccCtx.get()).getNamespace(
                deleteNamespaceRef
            );
            if (optNamespace.isPresent())
            {
                Iterator<String> keysIterator = optNamespace.get().keysIterator();
                while (keysIterator.hasNext())
                {
                    apiCallRc.addEntries(deleteProp(keysIterator.next(), deleteNamespaceRef));
                }

                Iterator<String> iterateNamespaces = optNamespace.get().iterateNamespaces();
                while (iterateNamespaces.hasNext())
                {
                    apiCallRc.addEntries(deleteNamespace(deleteNamespaceRef + "/" + iterateNamespaces.next()));
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
        return apiCallRc;
    }

    private boolean setCtrlProp(AccessContext accCtx, String key, String value, String namespace)
            throws InvalidValueException, AccessDeniedException, DatabaseException, InvalidKeyException
    {
        String oldVal = systemConfRepository.setCtrlProp(accCtx, key, value, namespace);
        if (oldVal != null) {
            return !oldVal.equals(value);
        }
        return true;
    }

    private boolean setStltProp(AccessContext accCtx, String key, String value)
        throws InvalidValueException, AccessDeniedException, DatabaseException, InvalidKeyException
    {
        String oldVal = systemConfRepository.setStltProp(accCtx, key, value);
        if (oldVal != null) {
            return !oldVal.equals(value);
        }
        return true;
    }

    public ApiCallRc setProp(String key, String namespace, String value)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
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
            /*
             * StorDriver/Exos/<EnclosureName>/<ExosCtrlName>/ ... must be ignored.
             * on ctrl level only.
             * on other levels whitelisting is active
             */
            ignoredKeys.add(ApiConsts.NAMESPC_EXOS + "/");
            if (whitelistProps.isAllowed(LinStorObject.CONTROLLER, ignoredKeys, fullKey, value, false))
            {
                boolean notifyStlts = false;
                String normalized = whitelistProps.normalize(LinStorObject.CONTROLLER, fullKey, value);
                if (fullKey.startsWith(ApiConsts.NAMESPC_REST + '/') || fullKey.startsWith(ApiConsts.NAMESPC_AUTOPLACER + "/"))
                {
                    notifyStlts = setCtrlProp(peerAccCtx.get(), key, normalized, namespace);
                }
                else
                {
                    switch (fullKey)
                    {
                        case ApiConsts.KEY_TCP_PORT_AUTO_RANGE:
                            setTcpPort(key, namespace, normalized, tcpPortPool, apiCallRc);
                            break;
                        case ApiConsts.KEY_MINOR_NR_AUTO_RANGE:
                            setMinorNr(key, namespace, normalized, apiCallRc);
                            break;
                        case ApiConsts.NAMESPC_SNAPSHOT_SHIPPING + "/" + ApiConsts.KEY_TCP_PORT_RANGE:
                            setTcpPort(key, namespace, normalized, snapShipPortPool, apiCallRc);
                            break;
                        case ApiConsts.KEY_SEARCH_DOMAIN: // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER: // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_QUORUM:
                            notifyStlts = setCtrlProp(peerAccCtx.get(), key, normalized, namespace);
                            break;
                        case ApiConsts.KEY_UPDATE_CACHE_INTERVAL: // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_AUTO_EVICT_AFTER_TIME: // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_AUTO_EVICT_MAX_DISCONNECTED_NODES: // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_AUTO_EVICT_MIN_REPLICA_COUNT:
                            setCtrlProp(peerAccCtx.get(), key, normalized, namespace); // no need to update stlts
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
                        default:
                            notifyStlts = setStltProp(peerAccCtx.get(), fullKey, normalized);
                            break;
                    }
                }
                transMgrProvider.get().commit();

                if (notifyStlts) {
                    updateSatelliteConf();
                }

                apiCallRc.addEntry(
                    "Successfully set property '" + fullKey + "' to value '" + normalized + "'",
                    ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT | ApiConsts.CREATED
                );
            }
            else
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                if (whitelistProps.isKeyKnown(LinStorObject.CONTROLLER, fullKey))
                {
                    entry.setMessage("The value '" + value + "' is not valid.");
                    entry.setDetails(
                        whitelistProps.getErrMsg(LinStorObject.CONTROLLER, fullKey)
                    );
                }
                else
                {
                    entry.setMessage("The key '" + fullKey + "' is not whitelisted");
                }
                entry.setReturnCode(ApiConsts.FAIL_INVLD_PROP | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT);
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
            }
            else
            {
                errorMsg = "An unknown error occurred while setting controller config prop with key '" +
                    key + "' in namespace '" + namespace + "' with value '" + value + "'.";
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

    public ApiCallRc deleteProp(String key, String namespace)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
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
            boolean isPropWhitelisted = whitelistProps.isAllowed(
                LinStorObject.CONTROLLER,
                Collections.singletonList(ApiConsts.NAMESPC_AUXILIARY + "/"),
                fullKey,
                null,
                false
            );
            if (isPropWhitelisted)
            {
                boolean notifyStlts = false;
                String oldValue = systemConfRepository.removeCtrlProp(peerAccCtx.get(), key, namespace);
                systemConfRepository.removeStltProp(peerAccCtx.get(), key, namespace);

                if (oldValue != null)
                {
                    notifyStlts = true;
                    switch (fullKey)
                    {
                        case ApiConsts.KEY_TCP_PORT_AUTO_RANGE:
                            tcpPortPool.reloadRange();
                            break;
                        case ApiConsts.KEY_MINOR_NR_AUTO_RANGE:
                            minorNrPool.reloadRange();
                            break;
                        case ApiConsts.KEY_TCP_PORT_RANGE:
                            snapShipPortPool.reloadRange();
                            break;
                        // TODO: check for other properties
                        default:
                            // ignore - for now
                    }
                }

                transMgrProvider.get().commit();

                if (notifyStlts) {
                    updateSatelliteConf();
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
            if (exc instanceof DatabaseException)
            {
                errorMsg = ResponseUtils.getSqlMsg(
                    "Deleting controller config prop with key '" + key + "' in namespace '" + namespace +
                    "'."
                );
                rc = ApiConsts.FAIL_SQL;
            }
            else
            {
                errorMsg = "An unknown error occurred while deleting controller config prop with key '" +
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
        return apiCallRc;
    }

    public ApiCallRc enterPassphrase(String passphrase)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            Props namespace = encHelper.getEncryptedNamespace(peerAccCtx.get());
            if (namespace == null || namespace.isEmpty())
            {
                ResponseUtils.reportStatic(
                    null,
                    EncryptionHelper.NAMESPACE_ENCRYPTED + " namespace is empty",
                    ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                    null, // objRefs
                    apiCallRc,
                    errorReporter,
                    peerAccCtx.get(),
                    peerProvider.get()
                );
            }
            else
            {
                byte[] decryptedMasterKey = encHelper.getDecryptedMasterKey(namespace, passphrase);
                if (decryptedMasterKey != null)
                {
                    encHelper.setCryptKey(decryptedMasterKey);

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
                apiCallRc
            );
        }
        catch (LinStorException exc)
        {
            ResponseUtils.reportStatic(
                exc,
                "Unknown error occurred while validating the passphrase",
                ApiConsts.FAIL_UNKNOWN_ERROR,
                null,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }

        return apiCallRc;
    }

    public ApiCallRc setPassphrase(String newPassphrase, String oldPassphrase)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        long mask = ApiConsts.MASK_CTRL_CONF;
        try
        {
            Props namespace = encHelper.getEncryptedNamespace(peerAccCtx.get());

            if (oldPassphrase == null)
            {
                mask |= ApiConsts.MASK_CRT;
                if (namespace == null || namespace.getProp(EncryptionHelper.KEY_CRYPT_KEY) == null)
                {
                    // no oldPassphrase and empty namespace means that
                    // this is the initial passphrase
                    byte[] masterKey = encHelper.generateSecret();
                    encHelper.setPassphraseImpl(
                        newPassphrase,
                        masterKey,
                        peerAccCtx.get()
                    );
                    encHelper.setCryptKey(masterKey);
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
                        "Coult not create new crypt passphrase as it already exists",
                        "A passphrase was already defined",
                        null,
                        "Use the crypt-modify-passphrase command instead of crypt-create-passphrase",
                        mask | ApiConsts.FAIL_EXISTS_CRYPT_PASSPHRASE,
                        new HashMap<>(),
                        null, // errorId
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
                        "Coult not modify crypt passphrase as it does not exist",
                        "No passphrase was defined yet",
                        null,
                        "Use the crypt-create-passphrase command instead of crypt-modify-passphrase",
                        mask | ApiConsts.FAIL_EXISTS_CRYPT_PASSPHRASE,
                        new HashMap<>(),
                        null, // errorId
                        apiCallRc
                    );
                }
                else
                {
                    byte[] decryptedMasterKey = encHelper.getDecryptedMasterKey(
                        namespace,
                        oldPassphrase
                    );
                    if (decryptedMasterKey != null)
                    {
                        encHelper.setPassphraseImpl(newPassphrase, decryptedMasterKey, peerAccCtx.get());
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
                apiCallRc
            );
        }
        catch (LinStorException exc)
        {
            ResponseUtils.reportStatic(
                exc,
                "An unknown exception occurred while setting the passphrase",
                ApiConsts.FAIL_UNKNOWN_ERROR,
                null,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        return apiCallRc;
    }

    private void setTcpPort(
        String key,
        String namespace,
        String value,
        DynamicNumberPool portPool,
        ApiCallRcImpl apiCallRc
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
                systemConfRepository.setCtrlProp(peerAccCtx.get(), key, value, namespace);
                portPool.reloadRange();

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
                rc = ApiConsts.FAIL_INVLD_TCP_PORT;
            }
            else
            if (exc instanceof ValueOutOfRangeException)
            {
                errorMsg = "The given tcp port number is not valid: '" + strTcpPort + "'.";
                rc = ApiConsts.FAIL_INVLD_TCP_PORT;
            }
            else
            {
                errorMsg = "An unknown exception occurred verifying the given TCP port '" + strTcpPort +
                    "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

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
        ApiCallRcImpl apiCallRc
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
                systemConfRepository.setCtrlProp(peerAccCtx.get(), key, value, namespace);
                minorNrPool.reloadRange();

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
                rc = ApiConsts.FAIL_INVLD_MINOR_NR;
            }
            else
            if (exc instanceof ValueOutOfRangeException)
            {
                errorMsg = "The given minor number is not valid: '" + strMinorNr + "'.";
                rc = ApiConsts.FAIL_INVLD_MINOR_NR;
            }
            else
            {
                errorMsg = "An unknown exception occurred verifying the given minor number'" + strMinorNr +
                    "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

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
