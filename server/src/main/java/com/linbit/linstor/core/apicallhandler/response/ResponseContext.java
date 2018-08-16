package com.linbit.linstor.core.apicallhandler.response;

import java.util.Map;

public class ResponseContext
{
    private final OperationDescription operationDescription;
    private final String objectDescription;
    private final String objectDescriptionInline;
    private final long opMask;
    private final long objMask;
    private final Map<String, String> objRefs;

    public ResponseContext(
        ApiOperation apiOperation,
        String objectDescriptionRef,
        String objectDescriptionInlineRef,
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
        String objectDescriptionRef,
        String objectDescriptionInlineRef,
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

    public String getObjectDescription()
    {
        return objectDescription;
    }

    public String getObjectDescriptionInline()
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
