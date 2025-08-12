package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public enum ZfsDeleteStrategy
{
    DELETE,
    RENAME,
    DYNAMIC;

    public static final String FULL_KEY_ZFS_DELETE_STRATEGY = ApiConsts.NAMESPC_STORAGE_DRIVER +
        ReadOnlyProps.PATH_SEPARATOR + ApiConsts.NAMESPC_ZFS +
        ReadOnlyProps.PATH_SEPARATOR + ApiConsts.KEY_ZFS_DELETE_STRATEGY;

    public static ZfsDeleteStrategy getStrat(
        ResourceDefinition rscDfnRef,
        ReadOnlyProps ctrlPropsRef,
        AccessContext accCtxRef
    )
        throws AccessDeniedException
    {
        try
        {
            return parseStrat(
                new PriorityProps(
                    rscDfnRef.getProps(accCtxRef),
                    rscDfnRef.getResourceGroup().getProps(accCtxRef),
                    ctrlPropsRef
                ).getProp(FULL_KEY_ZFS_DELETE_STRATEGY)
            );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public static ZfsDeleteStrategy parseStrat(@Nullable String value)
    {
        ZfsDeleteStrategy ret = DYNAMIC;
        if (value != null)
        {
            for (ZfsDeleteStrategy strat : values())
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
