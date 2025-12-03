package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.ErrorContextSupplier;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiCallRcImpl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class ApiRcException extends ApiException implements ErrorContextSupplier
{
    private static final long serialVersionUID = -4157838918419476508L;

    private final ApiCallRc apiCallRc;

    private final boolean hasContext;

    public ApiRcException(ApiCallRc.RcEntry rcEntryRef)
    {
        this(rcEntryRef, null);
    }

    public ApiRcException(ApiCallRc.RcEntry rcEntryRef, @Nullable Throwable throwableRef)
    {
        this(ApiCallRcImpl.singletonApiCallRc(rcEntryRef), throwableRef, false);
    }

    public ApiRcException(ApiCallRc apiCallRcRef)
    {
        this(apiCallRcRef, null, false);
    }

    public ApiRcException(ApiCallRc apiCallRcRef, @Nullable Throwable throwable, boolean hasContextRef)
    {
        super(
            apiCallRcRef.stream()
                .map(ApiCallRc.RcEntry::getMessage)
                .collect(Collectors.joining("; ")),
            throwable
        );
        apiCallRc = apiCallRcRef;
        hasContext = hasContextRef;
    }

    public ApiCallRc getApiCallRc()
    {
        return apiCallRc;
    }

    public boolean hasContext()
    {
        return hasContext;
    }

    @Override
    public String getErrorContext()
    {
        StringBuilder sb = new StringBuilder("ApiRcException entries: \n");
        int entryNr = 1;

        boolean printNumber = apiCallRc.size() > 1;
        for (RcEntry entry : apiCallRc)
        {
            if (printNumber)
            {
                sb.append("Nr: ").append(entryNr).append("\n");
            }
            if (entry.getMessage() != null)
            {
                sb.append("  Message:     ").append(entry.getMessage()).append("\n");
            }
            if (entry.getCause() != null)
            {
                sb.append("  Cause:       ").append(entry.getCause()).append("\n");
            }
            if (entry.getCorrection() != null)
            {
                sb.append("  Correction:  ").append(entry.getCorrection()).append("\n");
            }
            if (entry.getDetails() != null)
            {
                sb.append("  Details:     ").append(entry.getDetails()).append("\n");
            }
            sb.append("  NumericCode: ").append(entry.getReturnCode()).append("\n");
            Map<String, String> objRefs = entry.getObjRefs();
            if (objRefs != null && !objRefs.isEmpty())
            {
                sb.append("  Object References:");
                for (Entry<String, String> objRefEntry : objRefs.entrySet())
                {
                    sb.append("    ").append(objRefEntry.getKey()).append(": ").append(objRefEntry.getValue());
                }
            }
            entryNr++;
        }
        sb.append("\n");

        return sb.toString();
    }

    @Override
    public boolean hasErrorContext()
    {
        return !getApiCallRc().isEmpty();
    }
}
