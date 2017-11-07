package com.linbit.drbdmanage.api;

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

        public void setReturnCode(long returnCode)
        {
            this.returnCode = returnCode;
        }

        public void setReturnCodeBit(long bitMask)
        {
            this.returnCode |= bitMask;
        }

        public void setMessageFormat(String messageFormat)
        {
            this.messageFormat = messageFormat;
        }

        public void setCauseFormat(String causeFormat)
        {
            this.causeFormat = causeFormat;
        }

        public void setCorrectionFormat(String correctionFormat)
        {
            this.correctionFormat = correctionFormat;
        }

        public void setDetailsFormat(String detailsFormat)
        {
            this.detailsFormat = detailsFormat;
        }

        public void putObjRef(String key, String value)
        {
            objRefs.put(key, value);
        }

        public void putVariable(String key, String value)
        {
            variables.put(key, value);
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
    }

    public static class RcGenerator
    {

    }


}
