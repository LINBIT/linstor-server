package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.annotation.Nullable;

import java.util.Map;

public class ResponseContext
{
    private final OperationDescription operationDescription;
    private final @Nullable String objectDescription;
    private final @Nullable String objectDescriptionInline;
    private final long opMask;
    private final long objMask;
    private final Map<String, String> objRefs;

    public ResponseContext(
        ApiOperation apiOperation,
        @Nullable String objectDescriptionRef,
        @Nullable String objectDescriptionInlineRef,
        long objMaskRef,
        Map<String, String> objRefsRef
    )
    {
        this(
            apiOperation.getDescription(),
            objectDescriptionRef,
            objectDescriptionInlineRef,
            apiOperation.getOpMask(),
            objMaskRef,
            objRefsRef
        );
    }

    public ResponseContext(
        OperationDescription operationDescriptionRef,
        @Nullable String objectDescriptionRef,
        @Nullable String objectDescriptionInlineRef,
        long opMaskRef,
        long objMaskRef,
        Map<String, String> objRefsRef
    )
    {
        operationDescription = operationDescriptionRef;
        objectDescription = objectDescriptionRef;
        objectDescriptionInline = objectDescriptionInlineRef;
        opMask = opMaskRef;
        objMask = objMaskRef;
        objRefs = objRefsRef;
    }

    public OperationDescription getOperationDescription()
    {
        return operationDescription;
    }

    public @Nullable String getObjectDescription()
    {
        return objectDescription;
    }

    public @Nullable String getObjectDescriptionInline()
    {
        return objectDescriptionInline;
    }

    public long getOpMask()
    {
        return opMask;
    }

    public long getObjMask()
    {
        return objMask;
    }

    public Map<String, String> getObjRefs()
    {
        return objRefs;
    }
}
