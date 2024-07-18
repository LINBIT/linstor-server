package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.ExosConnectionMapPojo;
import com.linbit.linstor.api.pojo.ExosDefaultsPojo;
import com.linbit.linstor.api.pojo.ExosEnclosureEventPojo;
import com.linbit.linstor.api.pojo.ExosEnclosureHealthPojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.exos.ExosEnclosurePingTask;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.exos.rest.ExosRestClient;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestEventsCollection;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestEventsCollection.ExosRestEvent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.ExosMappingManager;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import static com.linbit.locks.LockGuardFactory.LockObj.CTRL_CONFIG;
import static com.linbit.locks.LockGuardFactory.LockType.READ;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Deprecated(forRemoval = true)
@Singleton
public class CtrlExosApiCallHandler
{
    private static final int DEFAULT_EVENTS_COUNT = 20;
    private final ErrorReporter errorReporter;
    private final ExosEnclosurePingTask exosPingTask;
    private final SystemConfRepository systemConfRepository;
    private final CtrlTransactionHelper txHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlConfApiCallHandler ctrlConfApiCallHandler;
    private final NodeRepository nodeRepo;
    private final StorPoolDefinitionRepository storPoolRepo;
    private final ScopeRunner scopeRunner;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final AccessContext sysCtx;
    private Provider<AccessContext> peerCtx;

    @Inject
    public CtrlExosApiCallHandler(
        ErrorReporter errorReporterRef,
        ExosEnclosurePingTask exosPingTaskRef,
        SystemConfRepository systemConfRepositoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlConfApiCallHandler ctrlConfApiCallHandlerRef,
        NodeRepository nodeRepoRef,
        StorPoolDefinitionRepository storPoolRepoRef,
        ScopeRunner scopeRunnerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @SystemContext AccessContext sysCtxRef,
        @PeerContext Provider<AccessContext> peerCtxRef
    )
    {
        errorReporter = errorReporterRef;
        exosPingTask = exosPingTaskRef;
        systemConfRepository = systemConfRepositoryRef;
        txHelper = ctrlTransactionHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        ctrlConfApiCallHandler = ctrlConfApiCallHandlerRef;
        nodeRepo = nodeRepoRef;
        storPoolRepo = storPoolRepoRef;
        scopeRunner = scopeRunnerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        sysCtx = sysCtxRef;
        peerCtx = peerCtxRef;
    }

    public List<ExosEnclosureHealthPojo> listEnclosures(boolean nocacheRef)
    {
        return exosPingTask.getPojos(nocacheRef);
    }

    public ExosDefaultsPojo getDefaults()
    {
        ExosDefaultsPojo ret;
        try
        {
            ret = new ExosDefaultsPojo(
                getDfltProp(ApiConsts.KEY_STOR_POOL_EXOS_API_USER),
                getDfltProp(ApiConsts.KEY_STOR_POOL_EXOS_API_USER_ENV),
                getDfltProp(ApiConsts.KEY_STOR_POOL_EXOS_API_PASSWORD),
                getDfltProp(ApiConsts.KEY_STOR_POOL_EXOS_API_PASSWORD_ENV)
            );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "accessing controller properties",
                ApiConsts.FAIL_ACC_DENIED_CTRL_CFG
            );
        }
        return ret;
    }

    private String getDfltProp(String keySuffix) throws InvalidKeyException, AccessDeniedException
    {
        return systemConfRepository.getStltConfForView(peerCtx.get()).getProp(ApiConsts.NAMESPC_EXOS + "/" + keySuffix);
    }

    public ApiCallRcImpl modifyDefaults(
        String usernameRef,
        String usernameEnvRef,
        String passwordRef,
        String passwordEnvRef,
        List<String> unsetKeysRef
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (LockGuard lg = lockGuardFactory.build(WRITE, CTRL_CONFIG))
        {
            Map<String, String> map = new HashMap<>();
            map.put(ApiConsts.KEY_STOR_POOL_EXOS_API_USER, usernameRef);
            map.put(ApiConsts.KEY_STOR_POOL_EXOS_API_USER_ENV, usernameEnvRef);
            map.put(ApiConsts.KEY_STOR_POOL_EXOS_API_PASSWORD, passwordRef);
            map.put(ApiConsts.KEY_STOR_POOL_EXOS_API_PASSWORD_ENV, passwordEnvRef);

            AccessContext accCtx = peerCtx.get();
            List<String> lowercaseUnsetKeys = unsetKeysRef.stream().map(String::toLowerCase)
                .collect(Collectors.toList());
            for (Entry<String, String> entry : map.entrySet())
            {
                if (entry.getValue() != null)
                {
                    systemConfRepository.setStltProp(
                        accCtx,
                        ApiConsts.NAMESPC_EXOS + "/" + entry.getKey(),
                        entry.getValue()
                    );
                }

                if (lowercaseUnsetKeys.contains(entry.getKey().toLowerCase()))
                {
                    systemConfRepository.removeStltProp(accCtx, entry.getKey(), ApiConsts.NAMESPC_EXOS);
                }
            }

            txHelper.commit();

            ctrlConfApiCallHandler.updateSatelliteConf();

            exosPingTask.getPojos(true); // force recache

            apiCallRc.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.MASK_SUCCESS,
                    "Defaults successully updated"
                )
            );
        }
        catch (InvalidKeyException | AccessDeniedException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        return apiCallRc;
    }

    public List<ExosConnectionMapPojo> showMap()
    {
        List<ExosConnectionMapPojo> ret = new ArrayList<>();
        try (LockGuard lg = lockGuardFactory.build(READ, LockObj.NODES_MAP))
        {
            for (Node node : nodeRepo.getMapForView(sysCtx).values())
            {
                final String nodeName = node.getName().displayValue;
                Optional<Props> namespace = node.getProps(sysCtx).getNamespace(ApiConsts.NAMESPC_EXOS);
                if (namespace.isPresent())
                {
                    ReadOnlyProps exosNamespace = namespace.get();

                    Iterator<String> enclosureIt = exosNamespace.iterateNamespaces();
                    while (enclosureIt.hasNext())
                    {
                        final String enclosureName = enclosureIt.next();
                        final List<String> connectedPorts = new ArrayList<>();

                        for (Entry<String, String> entry : exosNamespace.map().entrySet())
                        {
                            if (entry.getValue().equals(ExosMappingManager.CONNECTED))
                            {
                                String[] parts = entry.getKey().split(ReadOnlyProps.PATH_SEPARATOR);
                                connectedPorts.add(parts[3] + parts[5]);
                            }
                        }

                        if (!connectedPorts.isEmpty())
                        {
                            ret.add(new ExosConnectionMapPojo(nodeName, enclosureName, connectedPorts));
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    public List<ExosEnclosureEventPojo> describeEnclosure(String enclosureRef, Integer countRef)
    {
        List<ExosEnclosureEventPojo> ret = new ArrayList<>();
        try (LockGuard lg = lockGuardFactory.build(READ, CTRL_CONFIG))
        {
            ExosRestClient client = exosPingTask.getClient(enclosureRef);
            if (client != null)
            {
                ExosRestEventsCollection exosEvents = client.showEvents(
                    countRef == null ? DEFAULT_EVENTS_COUNT : countRef
                );

                for (ExosRestEvent event : exosEvents.events)
                {
                    ret.add(
                        new ExosEnclosureEventPojo(
                            event.severity,
                            event.eventId,
                            event.controller,
                            event.timeStamp,
                            event.timeStampNumeric,
                            event.message,
                            event.additionalInformation,
                            event.recommendedAction
                        )
                    );
                }
            }
            else
            {
                ret.add(
                    new ExosEnclosureEventPojo(
                        "Linstor-ERROR",
                        null,
                        null,
                        null,
                        null,
                        "Enclosure does not exist",
                        null,
                        null
                    )
                );
            }
        }
        catch (StorageException exc)
        {
            ret.add(
                new ExosEnclosureEventPojo(
                    "Linstor-ERROR",
                    null,
                    null,
                    null,
                    null,
                    "Failed to fetch events",
                    exc.getMessage(),
                    null
                )
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    public ApiCallRcImpl createEnclosure(
        String enclosureRef,
        String ctrlAIpRef,
        String ctrlBIpRef,
        String passwordRef,
        String passwordEnvRef,
        String usernameRef,
        String usernameEnvRef
    )
    {
        return modImplEnclosure(
            enclosureRef,
            ctrlAIpRef,
            ctrlBIpRef,
            usernameRef,
            usernameEnvRef,
            passwordRef,
            passwordEnvRef,
            true
        );
    }

    public ApiCallRcImpl modifyEnclosure(
        String enclosureRef,
        String ctrlAIpRef,
        String ctrlBIpRef,
        String passwordRef,
        String passwordEnvRef,
        String usernameRef,
        String usernameEnvRef
    )
    {
        return modImplEnclosure(
            enclosureRef,
            ctrlAIpRef,
            ctrlBIpRef,
            usernameRef,
            usernameEnvRef,
            passwordRef,
            passwordEnvRef,
            false
        );
    }

    private ApiCallRcImpl modImplEnclosure(
        String enclosureRef,
        String ctrlAIpRef,
        String ctrlBIpRef,
        String usernameRef,
        String usernameEnvRef,
        String passwordRef,
        String passwordEnvRef,
        boolean create
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (LockGuard lg = lockGuardFactory.build(WRITE, CTRL_CONFIG))
        {
            ReadOnlyProps stltPropsView = systemConfRepository.getStltConfForView(sysCtx);
            boolean enclosureNamespaceExists = stltPropsView
                .getNamespace(ApiConsts.NAMESPC_EXOS + "/" + enclosureRef).isPresent();
            if (create && enclosureNamespaceExists)
            {
                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_EXOS_ENCLOSURE,
                        String.format("An Exos enclosure with the name %s already exists", enclosureRef)
                    )
                );
            }
            else if (!create && !enclosureNamespaceExists)
            {
                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_EXOS_ENCLOSURE,
                        String.format("An Exos enclosure with the name %s does not exist", enclosureRef)
                    )
                );
            }
            else if (enclosureRef == null || enclosureRef.isEmpty())
            {
                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_CONF,
                        String.format("Enclosure name must not be empty")
                    )
                );
            }
            else if (create && (ctrlAIpRef == null || ctrlAIpRef.isEmpty()))
            {
                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_CONF,
                        String.format("At least controller A IP must not be empty")
                    )
                );
            }
            else
            {
                setIfNotNull(
                    enclosureRef + "/" + ExosRestClient.CONTROLLERS[0] + "/" + ApiConsts.KEY_STOR_POOL_EXOS_API_IP,
                    ctrlAIpRef
                );
                setIfNotNull(
                    enclosureRef + "/" + ExosRestClient.CONTROLLERS[1] + "/" + ApiConsts.KEY_STOR_POOL_EXOS_API_IP,
                    ctrlBIpRef
                );
                setIfNotNull(
                    enclosureRef + "/" + ApiConsts.KEY_STOR_POOL_EXOS_API_USER,
                    usernameRef
                );
                setIfNotNull(
                    enclosureRef + "/" + ApiConsts.KEY_STOR_POOL_EXOS_API_USER_ENV,
                    usernameEnvRef
                );
                setIfNotNull(
                    enclosureRef + "/" + ApiConsts.KEY_STOR_POOL_EXOS_API_PASSWORD,
                    passwordRef
                );
                setIfNotNull(
                    enclosureRef + "/" + ApiConsts.KEY_STOR_POOL_EXOS_API_PASSWORD_ENV,
                    passwordEnvRef
                );

                txHelper.commit();

                exosPingTask.getPojos(true); // force recache

                ctrlConfApiCallHandler.updateSatelliteConf();

                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.MASK_SUCCESS,
                        String.format(
                            "Exos Enclosure %s successfully %s",
                            enclosureRef,
                            create ? "created" : "modified"
                        )
                    )
                );
            }
        }
        catch (InvalidKeyException | AccessDeniedException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        return apiCallRc;
    }

    private void setIfNotNull(String keySuffix, String value)
        throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException
    {
        if (value != null)
        {
            systemConfRepository.setStltProp(sysCtx, ApiConsts.NAMESPC_EXOS + "/" + keySuffix, value);
        }
    }

    public ApiCallRcImpl deleteEnclosure(String enclosureRef)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (LockGuard lg = lockGuardFactory.create().write(CTRL_CONFIG).read(LockObj.STOR_POOL_DFN_MAP).build())
        {
            ReadOnlyProps stltPropsView = systemConfRepository.getStltConfForView(sysCtx);
            Optional<Props> exosEnclosureNamespace = stltPropsView.getNamespace(
                ApiConsts.NAMESPC_EXOS + "/" + enclosureRef
            );
            boolean enclosureNamespaceExists = exosEnclosureNamespace.isPresent();
            if (!enclosureNamespaceExists)
            {
                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.WARN_NOT_FOUND,
                        String.format("The Exos enclosure %s does not exist", enclosureRef)
                    )
                );
            }
            else
            {
                List<StorPool> storPoolsUsingEnclosure = new ArrayList<>();
                for (StorPoolDefinition storPoolDfn : storPoolRepo.getMapForView(sysCtx).values())
                {
                    Iterator<StorPool> storPoolIt = storPoolDfn.iterateStorPools(sysCtx);
                    while (storPoolIt.hasNext())
                    {
                        StorPool sp = storPoolIt.next();
                        if (sp.getSharedStorPoolName().getName().startsWith(enclosureRef.toUpperCase()))
                        {
                            storPoolsUsingEnclosure.add(sp);
                        }
                    }
                }

                if (!storPoolsUsingEnclosure.isEmpty())
                {
                    StringBuilder sb = new StringBuilder();
                    for (StorPool sp : storPoolsUsingEnclosure)
                    {
                        sb.append("\nStorage pool ").append(sp.getName().displayValue)
                            .append(" on Node ").append(sp.getNode().getName().displayValue);
                    }
                    apiCallRc.addEntry(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_IN_USE,
                            String.format(
                                "The enclosure %s is still in use at least from the following storage pools:%s",
                                enclosureRef,
                                sb.toString()
                            )
                        )
                    );
                }
                else
                {
                    Set<String> keys = new HashSet<>(exosEnclosureNamespace.get().keySet());
                    for (String key : keys)
                    {
                        systemConfRepository.removeStltProp(sysCtx, key, null);
                    }

                    exosPingTask.enclosureDeleted(enclosureRef);

                    txHelper.commit();

                    ctrlConfApiCallHandler.updateSatelliteConf();

                    apiCallRc.addEntry(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.MASK_SUCCESS,
                            String.format(
                                "Exos Enclosure %s successfully deleted",
                                enclosureRef
                            )
                        )
                    );
                }
            }
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        return apiCallRc;
    }

    public Object exec(String enclosureRef, List<String> cmdsRef)
    {
        Object ret;
        try (LockGuard lg = lockGuardFactory.build(READ, CTRL_CONFIG))
        {
            ExosRestClient client = exosPingTask.getClient(enclosureRef);
            if (client != null)
            {
                StringBuilder sb = new StringBuilder();
                for (String cmd : cmdsRef)
                {
                    sb.append(cmd).append("/");
                }
                if (sb.length() > 0)
                {
                    sb.setLength(sb.length() - 1);
                }
                ret = client.exec(sb.toString());
            }
            else
            {
                TreeMap<String, String> tmp = new TreeMap<>();
                tmp.put("Error", "Enclosure not found");
                ret = tmp;
            }
        }
        catch (StorageException exc)
        {
            errorReporter.reportError(exc);
            TreeMap<String, String> tmp = new TreeMap<>();
            tmp.put("Exception", exc.getMessage());
            ret = tmp;
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return ret;
    }
}
