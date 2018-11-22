package com.linbit.linstor.api;

import com.linbit.linstor.LinStorException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ApiCallRcImpl implements ApiCallRc
{
    private List<RcEntry> entries = new ArrayList<>();

    public ApiCallRcImpl()
    {
    }

    public ApiCallRcImpl(List<RcEntry> entriesRef)
    {
        entries.addAll(entriesRef);
    }

    public void addEntry(RcEntry entry)
    {
        entries.add(entry);
    }

    public void addEntry(String message, long returnCode)
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setMessage(message);
        entry.setReturnCode(returnCode);

        addEntry(entry);
    }

    public void addEntries(ApiCallRc apiCallRc)
    {
        entries.addAll(apiCallRc.getEntries());
    }

    @Override
    public List<RcEntry> getEntries()
    {
        return entries;
    }

    @Override
    public String toString()
    {
        return "ApiCallRcImpl{" +
            "entries=" + entries +
            '}';
    }

    public static ApiCallRcImpl singletonApiCallRc(RcEntry entry)
    {
        ApiCallRcImpl apiCallRcImpl = new ApiCallRcImpl();
        apiCallRcImpl.addEntry(entry);
        return apiCallRcImpl;
    }

    public static EntryBuilder entryBuilder(long returnCodeRef, String messageRef)
    {
        return new EntryBuilder(returnCodeRef, messageRef);
    }

    public static EntryBuilder entryBuilder(RcEntry source, Long returnCodeRef, String messageRef)
    {
        EntryBuilder entryBuilder = new EntryBuilder(
            returnCodeRef != null ? returnCodeRef : source.getReturnCode(),
            messageRef != null ? messageRef : source.getMessage()
        );
        entryBuilder.setCause(source.getCause());
        entryBuilder.setCorrection(source.getCorrection());
        entryBuilder.setDetails(source.getDetails());
        entryBuilder.putAllObjRefs(source.getObjRefs());
        entryBuilder.addAllErrorIds(source.getErrorIds());
        return entryBuilder;
    }

    public static ApiCallRcEntry simpleEntry(long returnCodeRef, String messageRef)
    {
        return entryBuilder(returnCodeRef, messageRef).build();
    }

    public static class ApiCallRcEntry implements ApiCallRc.RcEntry
    {
        private long returnCode = 0;
        private Map<String, String> objRefs = new HashMap<>();
        private String message;
        private String cause;
        private String correction;
        private String details;
        private Set<String> errorIds = new TreeSet<>();

        public void setReturnCode(long returnCodeRef)
        {
            returnCode = returnCodeRef;
        }

        public void setReturnCodeBit(long bitMask)
        {
            returnCode |= bitMask;
        }

        public void setMessage(String messageRef)
        {
            message = messageRef;
        }

        public void setCause(String causeRef)
        {
            cause = causeRef;
        }

        public void setCorrection(String correctionRef)
        {
            correction = correctionRef;
        }

        public void setDetails(String detailsRef)
        {
            details = detailsRef;
        }

        public void putObjRef(String key, String value)
        {
            objRefs.put(key, value);
        }

        public void putAllObjRef(Map<String, String> map)
        {
            objRefs.putAll(map);
        }

        public void addErrorId(String errorId)
        {
            errorIds.add(errorId);
        }

        public void addAllErrorIds(Set<String> errorIdsRef)
        {
            errorIds.addAll(errorIdsRef);
        }

        @Override
        public long getReturnCode()
        {
            return returnCode;
        }

        @Override
        public Map<String, String> getObjRefs()
        {
            return objRefs;
        }

        @Override
        public String getMessage()
        {
            return message;
        }

        @Override
        public String getCause()
        {
            return cause;
        }

        @Override
        public String getCorrection()
        {
            return correction;
        }

        @Override
        public String getDetails()
        {
            return details;
        }

        @Override
        public Set<String> getErrorIds()
        {
            return errorIds;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            ApiRcUtils.appendReadableRetCode(sb, returnCode);

            return "ApiCallRcEntry{" +
                "returnCode=" + sb.toString() +
                ", objRefs=" + objRefs +
                ", message='" + message + '\'' +
                ", cause='" + cause + '\'' +
                ", correction='" + correction + '\'' +
                ", details='" + details + '\'' +
                ", errorIds=" + errorIds +
                '}';
        }
    }

    public static class EntryBuilder
    {
        private final long returnCode;

        private final String message;

        private String cause;

        private String correction;

        private String details;

        private Map<String, String> objRefs = new TreeMap<>();

        private Set<String> errorIds = new TreeSet<>();

        private EntryBuilder(long returnCodeRef, String messageRef)
        {
            returnCode = returnCodeRef;
            message = messageRef;
        }

        public EntryBuilder setCause(String causeRef)
        {
            cause = causeRef;
            return this;
        }

        public EntryBuilder setCorrection(String correctionRef)
        {
            correction = correctionRef;
            return this;
        }

        public EntryBuilder setDetails(String detailsRef)
        {
            details = detailsRef;
            return this;
        }

        public EntryBuilder putObjRef(String key, String value)
        {
            objRefs.put(key, value);
            return this;
        }

        public EntryBuilder putAllObjRefs(Map<String, String> objRefsRef)
        {
            objRefs.putAll(objRefsRef);
            return this;
        }

        public EntryBuilder addErrorId(String errorId)
        {
            errorIds.add(errorId);
            return this;
        }

        public EntryBuilder addAllErrorIds(Collection<String> errorIdsRef)
        {
            errorIds.addAll(errorIdsRef);
            return this;
        }

        public ApiCallRcEntry build()
        {
            ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
            entry.setReturnCode(returnCode);
            entry.setMessage(message);
            entry.setCause(cause);
            entry.setCorrection(correction);
            entry.setDetails(details);
            entry.putAllObjRef(objRefs);
            entry.addAllErrorIds(errorIds);
            return entry;
        }
    }

    public static RcEntry copyFromLinstorExc(long retCode, LinStorException linstorExc)
    {
        ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCode(retCode);
        if (linstorExc.getDescriptionText() != null)
        {
            entry.setMessage(linstorExc.getDescriptionText());
        }
        else
        {
            entry.setMessage(linstorExc.getMessage());
        }

        if (linstorExc.getCauseText() != null)
        {
            entry.setCause(linstorExc.getCauseText());
        }
        if (linstorExc.getCorrectionText() != null)
        {
            entry.setCorrection(linstorExc.getCorrectionText());
        }
        if (linstorExc.getDetailsText() != null)
        {
            entry.setDetails(linstorExc.getDetailsText());
        }
        return entry;
    }
}
