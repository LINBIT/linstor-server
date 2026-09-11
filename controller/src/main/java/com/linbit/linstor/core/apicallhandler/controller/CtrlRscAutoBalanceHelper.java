package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.propscon.Props;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.StringUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collections;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscAutoBalanceHelper
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final SystemConfRepository systemConfRepository;
    private final CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandler;

    @Inject
    public CtrlRscAutoBalanceHelper(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        SystemConfRepository systemConfRepositoryRef,
        CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandlerRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        systemConfRepository = systemConfRepositoryRef;
        ctrlRscAutoPlaceApiCallHandler = ctrlRscAutoPlaceApiCallHandlerRef;
    }

    public Flux<ApiCallRc> balanceAfterOperation(
        ResourceDefinition rscDfn,
        String propKey,
        String propNamespace
    )
    {
        Props rscGrpProps = rscDfn.getResourceGroup().getProps();
        PriorityProps prioProps = new PriorityProps(
            rscDfn.getProps(), rscGrpProps, systemConfRepository.getCtrlConfForView()
        );
        Flux<ApiCallRc> flux = Flux.empty();
        if (StringUtils.propTrueOrYes(prioProps.getProp(propKey, propNamespace, "false")))
        {
            flux = scopeRunner
                .fluxInTransactionalScope(
                    "Balance resource (autoplace+1)",
                    lockGuardFactory.create()
                        .write(LockGuardFactory.LockObj.RSC_DFN_MAP, LockGuardFactory.LockObj.NODES_MAP)
                        .buildDeferred(),
                    () -> balanceInTransaction(rscDfn)
                );
        }
        return flux;
    }

    private Flux<ApiCallRc> balanceInTransaction(ResourceDefinition rscDfn)
    {
        AutoSelectFilterPojo selFilter = AutoSelectFilterPojo.copy(
            rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData());
        selFilter.setAdditionalPlaceCount(1);
        return ctrlRscAutoPlaceApiCallHandler.autoPlace(
            rscDfn.getName().displayValue,
            selFilter,
            false,
            Collections.emptyList()
        );
    }
}
