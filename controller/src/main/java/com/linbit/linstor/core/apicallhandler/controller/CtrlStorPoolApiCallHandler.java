package com.linbit.linstor.core.apicallhandler.controller;

import static com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper.getStorPoolDescription;
import static com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper.getStorPoolDescriptionInline;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinitionRepository;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.FreeSpacePojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
public class CtrlStorPoolApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final StorPoolHelper storPoolHelper;

    @Inject
    public CtrlStorPoolApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        StorPoolHelper storPoolHelperRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        storPoolHelper = storPoolHelperRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    public ApiCallRc createStorPool(
        String nodeNameStr,
        String storPoolNameStr,
        String driver,
        String freeSpaceMgrNameStr,
        Map<String, String> storPoolPropsMap
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeStorPoolContext(
            ApiOperation.makeRegisterOperation(),
            nodeNameStr,
            storPoolNameStr
        );

        try
        {
            // as the storage pool definition is implicitly created if it doesn't exist
            // we always will update the storPoolDfnMap even if not necessary
            // Therefore we need to be able to modify apiCtrlAccessors.storPoolDfnMap
            requireStorPoolDfnMapChangeAccess();

            StorPoolData storPool = storPoolHelper.createStorPool(
                nodeNameStr,
                storPoolNameStr,
                driver,
                freeSpaceMgrNameStr
            );
            ctrlPropsHelper.fillProperties(
                LinStorObject.STORAGEPOOL, storPoolPropsMap, getProps(storPool), ApiConsts.FAIL_ACC_DENIED_STOR_POOL);

            updateStorPoolDfnMap(storPool);

            ctrlTransactionHelper.commit();

            responseConverter.addWithDetail(
                responses, context, ctrlSatelliteUpdater.updateSatellite(storPool));

            responseConverter.addWithOp(responses, context,
                ApiSuccessUtils.defaultRegisteredEntry(storPool.getUuid(), getStorPoolDescriptionInline(storPool)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc modifyStorPool(
        UUID storPoolUuid,
        String nodeNameStr,
        String storPoolNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeStorPoolContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            storPoolNameStr
        );

        try
        {
            StorPoolData storPool = loadStorPool(nodeNameStr, storPoolNameStr, true);

            if (storPoolUuid != null && !storPoolUuid.equals(storPool.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_STOR_POOL,
                    "UUID-check failed"
                ));
            }

            Props props = getProps(storPool);
            Map<String, String> propsMap = props.map();

            ctrlPropsHelper.fillProperties(
                LinStorObject.STORAGEPOOL, overrideProps, getProps(storPool), ApiConsts.FAIL_ACC_DENIED_STOR_POOL);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                storPool.getUuid(), getStorPoolDescriptionInline(storPool)));
            responseConverter.addWithDetail(
                responses, context, ctrlSatelliteUpdater.updateSatellite(storPool));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc deleteStorPool(
        String nodeNameStr,
        String storPoolNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeStorPoolContext(
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            storPoolNameStr
        );

        try
        {
            if (LinStor.DISKLESS_STOR_POOL_NAME.equalsIgnoreCase(storPoolNameStr))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_INVLD_STOR_POOL_NAME,
                        "The default diskless storage pool cannot be deleted!"
                    )
                    .build()
                );
            }

            StorPoolData storPool = loadStorPool(nodeNameStr, storPoolNameStr, false);

            if (storPool == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.WARN_NOT_FOUND,
                        "Deletion of " + getStorPoolDescriptionInline(nodeNameStr, storPoolNameStr) + " had no effect."
                    )
                    .setCause(firstLetterCaps(getStorPoolDescriptionInline(nodeNameStr, storPoolNameStr)) +
                        " does not exist.")
                    .build()
                );
            }
            Collection<Volume> volumes = getVolumes(storPool);
            if (!volumes.isEmpty())
            {
                StringBuilder volListSb = new StringBuilder();
                for (Volume vol : volumes)
                {
                    volListSb.append("\n   Node name: '")
                             .append(vol.getResource().getAssignedNode().getName().displayValue)
                             .append("', resource name: '")
                             .append(vol.getResource().getDefinition().getName().displayValue)
                             .append("', volume number: ")
                             .append(vol.getVolumeDefinition().getVolumeNumber().value);
                }

                String correction = volumes.size() == 1 ?
                    "Delete the listed volume first." :
                    "Delete the listed volumes first.";
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        String.format(
                                "The specified storage pool '%s' on node '%s' can not be deleted as " +
                                    "volumes are still using it.",
                                storPoolNameStr,
                                nodeNameStr
                            )
                    )
                    .setDetails("Volumes that are still using the storage pool: " + volListSb.toString())
                    .setCorrection(correction)
                    .build()
                );
            }
            else
            {
                UUID storPoolUuid = storPool.getUuid(); // cache storpool uuid to avoid access deleted storpool
                StorPoolName storPoolName = storPool.getName();
                Node node = storPool.getNode();
                delete(storPool);
                ctrlTransactionHelper.commit();

                responseConverter.addWithDetail(
                    responses,
                    context,
                    ctrlSatelliteUpdater.updateSatellite(node, storPoolName, storPoolUuid)
                );

                responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultDeletedEntry(
                    storPoolUuid, getStorPoolDescription(nodeNameStr, storPoolNameStr)));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public void respondStorPool(long apiCallId, UUID storPoolUuid, String storPoolNameStr)
    {
        try
        {
            StorPoolName storPoolName = new StorPoolName(storPoolNameStr);

            Peer currentPeer = peer.get();
            StorPool storPool = currentPeer.getNode().getStorPool(apiCtx, storPoolName);
            // TODO: check if the storPool has the same uuid as storPoolUuid
            if (storPool != null)
            {
                long fullSyncTimestamp = currentPeer.getFullSyncId();
                long updateId = currentPeer.getNextSerializerId();
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_STOR_POOL)
                        .storPoolData(storPool, fullSyncTimestamp, updateId)
                        .build()
                );
            }
            else
            {
                long fullSyncTimestamp = currentPeer.getFullSyncId();
                long updateId = currentPeer.getNextSerializerId();
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_STOR_POOL_DELETED)
                        .deletedStorPoolData(storPoolNameStr, fullSyncTimestamp, updateId)
                        .build()
                );
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Satellite requested data for invalid storpool name '" + storPoolNameStr + "'.",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested storpool data.",
                    accDeniedExc
                )
            );
        }
    }

    public void updateRealFreeSpace(List<FreeSpacePojo> freeSpacePojoList)
    {
        if (!peer.get().getNode().isDeleted())
        {
            String nodeName = peer.get().getNode().getName().displayValue;

            try
            {
                for (FreeSpacePojo freeSpacePojo : freeSpacePojoList)
                {
                    ResponseContext context = makeStorPoolContext(
                        ApiOperation.makeModifyOperation(),
                        peer.get().getNode().getName().displayValue,
                        freeSpacePojo.getStorPoolName()
                    );

                    try
                    {
                        StorPoolData storPool = loadStorPool(nodeName, freeSpacePojo.getStorPoolName(), true);
                        if (storPool.getUuid().equals(freeSpacePojo.getStorPoolUuid()))
                        {
                            setRealFreeSpace(storPool, freeSpacePojo.getFreeSpace());
                        }
                        else
                        {
                            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_UUID_STOR_POOL,
                                "UUIDs mismatched when updating free space of " + getStorPoolDescriptionInline(storPool)
                            ));
                        }
                    }
                    catch (Exception | ImplementationError exc)
                    {
                        // Add context to exception
                        throw new ApiRcException(
                            responseConverter.exceptionToResponse(peer.get(), context, exc), exc, true);
                    }
                }

                ctrlTransactionHelper.commit();
            }
            catch (ApiRcException exc)
            {
                ApiCallRc apiCallRc = exc.getApiCallRc();
                for (ApiCallRc.RcEntry entry : apiCallRc.getEntries())
                {
                    errorReporter.reportError(
                        exc instanceof ApiException && exc.getCause() != null ? exc.getCause() : exc,
                        peerAccCtx.get(),
                        peer.get(),
                        entry.getMessage()
                    );
                }
            }
        }
        // else: the node is deleted, thus if it still has any storpools left, those will
        // soon be deleted as well.
    }

    private void setRealFreeSpace(StorPoolData storPool, long freeSpace)
    {
        try
        {
            storPool.getFreeSpaceTracker().setFreeSpace(peerAccCtx.get(), freeSpace);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "update free space of free space manager '" +
                    storPool.getFreeSpaceTracker().getName().displayValue +
                    "'",
                ApiConsts.FAIL_ACC_DENIED_FREE_SPACE_MGR
            );
        }
    }

    private void requireStorPoolDfnMapChangeAccess()
    {
        try
        {
            storPoolDefinitionRepository.requireAccess(
                peerAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "change any storage pools",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
    }

    private void updateStorPoolDfnMap(StorPoolData storPool)
    {
        try
        {
            storPoolDefinitionRepository.put(
                apiCtx,
                storPool.getName(),
                storPool.getDefinition(apiCtx)
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
    }

    private Collection<Volume> getVolumes(StorPoolData storPool)
    {
        Collection<Volume> volumes;
        try
        {
            volumes = storPool.getVolumes(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the volumes of " + getStorPoolDescriptionInline(storPool),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return volumes;
    }

    private void delete(StorPoolData storPool)
    {
        try
        {
            storPool.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + StorPoolHelper.getStorPoolDescriptionInline(storPool),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private StorPoolData loadStorPool(String nodeNameStr, String storPoolNameStr, boolean failIfNull)
    {
        return ctrlApiDataLoader.loadStorPool(
            ctrlApiDataLoader.loadStorPoolDfn(storPoolNameStr, true),
            ctrlApiDataLoader.loadNode(nodeNameStr, true),
            failIfNull
        );
    }

    private Props getProps(StorPoolData storPool)
    {
        Props props;
        try
        {
            props = storPool.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties of storage pool '" + storPool.getName().displayValue +
                    "' on node '" + storPool.getNode().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return props;
    }

    private static ResponseContext makeStorPoolContext(
        ApiOperation operation,
        String nodeNameStr,
        String storPoolNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);
        objRefs.put(ApiConsts.KEY_STOR_POOL_DFN, storPoolNameStr);

        return new ResponseContext(
            operation,
            getStorPoolDescription(nodeNameStr, storPoolNameStr),
            getStorPoolDescriptionInline(nodeNameStr, storPoolNameStr),
            ApiConsts.MASK_STOR_POOL,
            objRefs
        );
    }
}
