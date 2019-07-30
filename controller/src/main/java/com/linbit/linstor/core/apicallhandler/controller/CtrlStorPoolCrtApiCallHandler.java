package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.StorPoolData;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Map;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolApiCallHandler.makeStorPoolContext;
import static com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper.getStorPoolDescriptionInline;

@Singleton
public class CtrlStorPoolCrtApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final StorPoolHelper storPoolHelper;

    @Inject
    public CtrlStorPoolCrtApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        StorPoolHelper storPoolHelperRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        storPoolHelper = storPoolHelperRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    public Flux<ApiCallRc> createStorPool(
        String nodeNameStr,
        String storPoolNameStr,
        DeviceProviderKind providerKindRef,
        String freeSpaceMgrNameStr,
        Map<String, String> storPoolPropsMap
    )
    {
        ResponseContext context = makeStorPoolContext(
            ApiOperation.makeRegisterOperation(),
            nodeNameStr,
            storPoolNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Create storage pool",
                lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.STOR_POOL_DFN_MAP),
                () -> createStorPoolInTransaction(
                    nodeNameStr,
                    storPoolNameStr,
                    providerKindRef,
                    freeSpaceMgrNameStr,
                    storPoolPropsMap,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createStorPoolInTransaction(
        String nodeNameStr,
        String storPoolNameStr,
        DeviceProviderKind deviceProviderKindRef,
        String freeSpaceMgrNameStr,
        Map<String, String> storPoolPropsMap,
        ResponseContext context
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        // as the storage pool definition is implicitly created if it doesn't exist
        // we always will update the storPoolDfnMap even if not necessary
        // Therefore we need to be able to modify apiCtrlAccessors.storPoolDfnMap
        requireStorPoolDfnMapChangeAccess();

        StorPoolData storPool = storPoolHelper.createStorPool(
            nodeNameStr,
            storPoolNameStr,
            deviceProviderKindRef,
            freeSpaceMgrNameStr
        );
        ctrlPropsHelper.fillProperties(
            LinStorObject.STORAGEPOOL,
            storPoolPropsMap, ctrlPropsHelper.getProps(storPool),
            ApiConsts.FAIL_ACC_DENIED_STOR_POOL
        );

        updateStorPoolDfnMap(storPool);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> updateResponses = ctrlSatelliteUpdateCaller
                .updateSatellite(storPool)
                .onErrorResume(
                        ApiRcException.class,
                        apiRcException -> Flux.just(apiRcException.getApiCallRc())
                );

        responseConverter.addWithOp(responses, context,
            ApiSuccessUtils.defaultRegisteredEntry(storPool.getUuid(), getStorPoolDescriptionInline(storPool)));

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(updateResponses);
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
}
