package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.api.ApiConsts;

public class ApiOperation
{
    private final long opMask;

    private final OperationDescription description;

    public ApiOperation(long opMaskRef, OperationDescription descriptionRef)
    {
        opMask = opMaskRef;
        description = descriptionRef;
    }

    public long getOpMask()
    {
        return opMask;
    }

    public OperationDescription getDescription()
    {
        return description;
    }

    public static ApiOperation makeCreateOperation()
    {
        return new ApiOperation(ApiConsts.MASK_CRT, new OperationDescription("creation", "creating"));
    }

    public static ApiOperation makeModifyOperation()
    {
        return new ApiOperation(ApiConsts.MASK_MOD, new OperationDescription("modification", "modifying"));
    }

    public static ApiOperation makeDeleteOperation()
    {
        return new ApiOperation(ApiConsts.MASK_DEL, new OperationDescription("deletion", "deleting"));
    }
}
