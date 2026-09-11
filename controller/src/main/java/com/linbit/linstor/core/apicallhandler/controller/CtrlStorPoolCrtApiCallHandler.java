package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolApiCallHandler.makeStorPoolContext;
import static com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper.getStorPoolDescriptionInline;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collections;
import java.util.Map;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlStorPoolCrtApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final StorPoolHelper storPoolHelper;
    private final CtrlApiDataLoader dataLoader;

    @Inject
    public CtrlStorPoolCrtApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        StorPoolHelper storPoolHelperRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader dataLoaderRef,
        ErrorReporter errorReporterRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        storPoolHelper = storPoolHelperRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        dataLoader = dataLoaderRef;
        errorReporter = errorReporterRef;
    }

    public Flux<ApiCallRc> createStorPool(
        String nodeNameStr,
        String storPoolNameStr,
        DeviceProviderKind providerKindRef,
        @Nullable String sharedStorPoolNameStr,
        boolean externalLockingRef,
        Map<String, String> storPoolPropsMap,
        Flux<ApiCallRc> onError
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
                    sharedStorPoolNameStr,
                    externalLockingRef,
                    storPoolPropsMap,
                    context,
                    onError
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createStorPoolInTransaction(
        String nodeNameStr,
        String storPoolNameStr,
        DeviceProviderKind deviceProviderKindRef,
        @Nullable String sharedStorPoolNameStr,
        boolean externalLockingRef,
        Map<String, String> storPoolPropsMap,
        ResponseContext context,
        Flux<ApiCallRc> onError
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        Flux<ApiCallRc> flux;

        try
        {
            // as the storage pool definition is implicitly created if it doesn't exist
            // we always will update the storPoolDfnMap even if not necessary
            // Therefore we need to be able to modify apiCtrlAccessors.storPoolDfnMap
            requireStorPoolDfnMapChangeAccess();

            String modifiedNameStr = sharedStorPoolNameStr;
            switch (deviceProviderKindRef)
            {
                case EBS_INIT:
                {
                    String ebsRemoteName = storPoolPropsMap.get(
                        ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.NAMESPC_EBS + "/" + ApiConsts.KEY_REMOTE
                    );
                    if (ebsRemoteName == null || ebsRemoteName.isEmpty())
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_INVLD_PROP,
                                "Invalid EBS remote name: '" + ebsRemoteName + "'"
                            )
                        );
                    }
                    AbsRemote remote = dataLoader.loadRemote(ebsRemoteName);
                    if (!(remote instanceof EbsRemote))
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_INVLD_PROP,
                                "Remote '" + ebsRemoteName + "' is not an EBS remote"
                            )
                        );
                    }
                    break;
                }
                case EBS_TARGET:
                {
                    Node node = dataLoader.loadNode(nodeNameStr);
                    if (node.getStorPoolCount() > 0)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.MASK_ERROR,
                                "An EBS target can only have one storage pool. Please use the node create-ebs-target " +
                                    "command instead"
                            )
                        );
                    }
                    break;
                }
                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.MASK_ERROR,
                            "nice try :)"
                        )
                    );
                case DISKLESS: // fall-through
                case FILE: // fall-through
                case FILE_THIN: // fall-through
                case LVM: // fall-through
                case LVM_THIN: // fall-through
                case REMOTE_SPDK: // fall-through
                case SPDK: // fall-through
                case ZFS: // fall-through
                case ZFS_THIN: // fall-through
                case STORAGE_SPACES: // fall-through
                case STORAGE_SPACES_THIN: // fall-through
                default:
                    // no special checks
                    break;
            }

            StorPool storPool = storPoolHelper.createStorPool(
                nodeNameStr,
                storPoolNameStr,
                deviceProviderKindRef,
                modifiedNameStr,
                externalLockingRef
            );

            // with external locking the admin is responsible for the lock manager coping with the
            // provider; only pools LINSTOR arbitrates itself are restricted at creation
            if (storPool.usesLinstorLocking() && !deviceProviderKindRef.isSharedVolumeSupported())
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_STOR_DRIVER,
                    String.format("Storage driver %s does not support shared volumes", deviceProviderKindRef.name())
                ));
            }
            // check if specified preferred network interface exists
            ctrlPropsHelper.checkPrefNic(
                storPool.getNode(),
                storPoolPropsMap.get(ApiConsts.KEY_STOR_POOL_PREF_NIC),
                ApiConsts.MASK_STOR_POOL
            );
            ctrlPropsHelper.checkPrefNic(
                storPool.getNode(),
                storPoolPropsMap.get(ApiConsts.NAMESPC_NVME + "/" + ApiConsts.KEY_PREF_NIC),
                ApiConsts.MASK_STOR_POOL
            );
            // check if specified "outside address" network interface exists
            ctrlPropsHelper.checkPrefOutsideAddress(
                storPool.getNode(),
                storPoolPropsMap.get(
                    ApiConsts.NAMESPC_LINSTOR_DRBD + "/" + ApiConsts.KEY_DRBD_OUTSIDE_ADDRESS
                ),
                ApiConsts.MASK_STOR_POOL
            );

            ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.STOR_POOL,
                storPoolPropsMap, ctrlPropsHelper.getProps(storPool),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL,
                Collections.singletonList(ApiConsts.NAMESPC_SED + ReadOnlyProps.PATH_SEPARATOR)
            );

            updateStorPoolDfnMap(storPool);

            ctrlTransactionHelper.commit();

            errorReporter.logInfo("Storage pool created %s/%s/%s",
                nodeNameStr, storPoolNameStr, deviceProviderKindRef);

            Flux<ApiCallRc> updateResponses = ctrlSatelliteUpdateCaller
                .updateSatellite(storPool)
                .transform(
                    innerUpdateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        innerUpdateResponses,
                        storPoolNameStr,
                        Collections.emptyList(),
                        "Storage pool updated on {0}",
                        "Storage pool updated on {0}"
                    )
                )
                .onErrorResume(
                    ApiRcException.class,
                    apiRcException -> Flux.just(apiRcException.getApiCallRc())
                );

            responseConverter.addWithOp(responses, context,
                ApiSuccessUtils.defaultRegisteredEntry(storPool.getUuid(), getStorPoolDescriptionInline(storPool)));

            flux = Flux
                .<ApiCallRc>just(responses)
                .concatWith(updateResponses);
        }
        catch (InvalidNameException exc)
        {
            ApiCallRc.RcEntry errorRc = ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_STOR_POOL_NAME,
                exc.getMessage());

            responseConverter.addWithOp(responses, context, errorRc);
            flux = Flux.<ApiCallRc>just(responses).concatWith(onError);
        }
        catch (ApiRcException apiExc)
        {
            flux = Flux.<ApiCallRc>just(apiExc.getApiCallRc()).concatWith(onError);
        }

        return flux;
    }

    private void requireStorPoolDfnMapChangeAccess()
    {
    }

    private void updateStorPoolDfnMap(StorPool storPool)
    {
        storPoolDefinitionRepository.put(
            storPool.getName(),
            storPool.getDefinition()
        );
    }
}
