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
        entry.setMessageFormat(message);
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
        private String messageFormat;
        private String causeFormat;
        private String correctionFormat;
        private String detailsFormat;
        private Map<String, String> variables = new HashMap<>();

        public void setReturnCode(long returnCodeRef)
        {
            returnCode = returnCodeRef;
        }

        public void setReturnCodeBit(long bitMask)
        {
            returnCode |= bitMask;
        }

        public void setMessageFormat(String messageFormatRef)
        {
            messageFormat = messageFormatRef;
        }

        public void setCauseFormat(String causeFormatRef)
        {
            causeFormat = causeFormatRef;
        }

        public void setCorrectionFormat(String correctionFormatRef)
        {
            correctionFormat = correctionFormatRef;
        }

        public void setDetailsFormat(String detailsFormatRef)
        {
            detailsFormat = detailsFormatRef;
        }

        public void putObjRef(String key, String value)
        {
            objRefs.put(key, value);
        }

        public void putAllObjRef(Map<String, String> map)
        {
            objRefs.putAll(map);
        }

        public void putVariable(String key, String value)
        {
            variables.put(key, value);
        }

        public void putAllVariables(Map<String, String> map)
        {
            variables.putAll(map);
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
        public String getMessageFormat()
        {
            return messageFormat;
        }

        @Override
        public String getCauseFormat()
        {
            return causeFormat;
        }

        @Override
        public String getCorrectionFormat()
        {
            return correctionFormat;
        }

        @Override
        public String getDetailsFormat()
        {
            return detailsFormat;
        }

        @Override
        public Map<String, String> getVariables()
        {
            return variables;
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
