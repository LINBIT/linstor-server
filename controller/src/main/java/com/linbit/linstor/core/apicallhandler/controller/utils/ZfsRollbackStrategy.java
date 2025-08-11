package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public enum ZfsRollbackStrategy
{
    ROLLBACK,
    CLONE,
    DYNAMIC;

    public static final String FULL_KEY_USE_ZFS_ROLLBACK_PROP = ApiConsts.NAMESPC_STORAGE_DRIVER +
        ReadOnlyProps.PATH_SEPARATOR + ApiConsts.NAMESPC_ZFS +
        ReadOnlyProps.PATH_SEPARATOR + ApiConsts.KEY_ZFS_ROLLBACK_STRATEGY;

    public static ZfsRollbackStrategy getStrat(
        @Nullable String rollbackStratFromClientRef,
        ResourceDefinition rscDfnRef,
        ReadOnlyProps ctrlPropsRef,
        AccessContext accCtxRef
    )
        throws InvalidKeyException, AccessDeniedException
    {
        return parseStrat(
            rollbackStratFromClientRef != null ?
                rollbackStratFromClientRef :
                new PriorityProps(
                    rscDfnRef.getProps(accCtxRef),
                    rscDfnRef.getResourceGroup().getProps(accCtxRef),
                    ctrlPropsRef
                ).getProp(FULL_KEY_USE_ZFS_ROLLBACK_PROP)
        );
    }

    public static ZfsRollbackStrategy parseStrat(@Nullable String value)
    {
        ZfsRollbackStrategy ret = DYNAMIC;
        if (value != null)
        {
            for (ZfsRollbackStrategy strat : values())
            {
                if (strat.name().equalsIgnoreCase(value))
                {
                    ret = strat;
                    break;
                }
            }
        }
        return ret;
    }
}
