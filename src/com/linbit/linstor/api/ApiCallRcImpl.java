package com.linbit.linstor.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApiCallRcImpl implements ApiCallRc
{
    private List<RcEntry> entries = new ArrayList<>();

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

    @Override
    public List<RcEntry> getEntries()
    {
        return entries;
    }

    public static class ApiCallRcEntry implements ApiCallRc.RcEntry
    {
        private long returnCode = 0;
        private Map<String, String> objRefs = new HashMap<>();
        private String message;
        private String cause;
        private String correction;
        private String details;

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
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            ApiRcUtils.appendReadableRetCode(sb, returnCode);
            return sb.toString();
        }
    }
}
