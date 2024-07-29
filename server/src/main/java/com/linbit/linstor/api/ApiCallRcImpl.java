package com.linbit.linstor.api;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.UnaryOperator;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class ApiCallRcImpl implements ApiCallRc
{
    private final List<RcEntry> entries = new ArrayList<>();

    public ApiCallRcImpl()
    {
    }

    public ApiCallRcImpl(RcEntry entry)
    {
        entries.add(entry);
    }

    public ApiCallRcImpl(List<RcEntry> entriesRef)
    {
        entries.addAll(entriesRef);
    }

    public void addEntry(RcEntry entry)
    {
        if (entry != null)
        {
            entries.add(entry);
        }
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
        entries.addAll(apiCallRc);
    }

    @Override
    public boolean hasErrors()
    {
        return entries.stream().anyMatch(RcEntry::isError);
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

    public static EntryBuilder entryBuilder(RcEntry source, @Nullable Long returnCodeRef, @Nullable String messageRef)
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
        entryBuilder.setSkipErrorReport(source.skipErrorReport());
        entryBuilder.setAppendObjectDescriptionToDetails(source.appendObjectDescrptionToDetails());
        return entryBuilder;
    }

    public static ApiCallRcImpl singleApiCallRc(long returnCode, String message)
    {
        return singletonApiCallRc(simpleEntry(returnCode, message));
    }

    public static ApiCallRcImpl singleApiCallRc(long returnCode, LinStorException linExc)
    {
        return singletonApiCallRc(ApiCallRcImpl.copyFromLinstorExc(returnCode, linExc));
    }

    public static ApiCallRcImpl singleApiCallRc(long returnCode, String message, String cause)
    {
        return singletonApiCallRc(simpleEntry(returnCode, message, cause));
    }

    public static ApiCallRcEntry simpleEntry(long returnCodeRef, String messageRef)
    {
        return entryBuilder(returnCodeRef, messageRef).build();
    }

    public static ApiCallRcEntry simpleEntry(long returnCodeRef, String messageRef, boolean skipErrorReport)
    {
        return entryBuilder(returnCodeRef, messageRef).setSkipErrorReport(skipErrorReport).build();
    }

    public static ApiCallRcEntry simpleEntry(long returnCode, String message, String cause)
    {
        return entryBuilder(returnCode, message).setCause(cause).build();
    }

    static class ZonedDTToString extends JsonSerializer<ZonedDateTime>
    {
        @Override
        public void serialize(ZonedDateTime tmpDt,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider)
            throws IOException
        {
            jsonGenerator.writeObject(tmpDt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        }
    }

    static class StringToZonedDT extends JsonDeserializer<ZonedDateTime>
    {
        @Override
        public ZonedDateTime deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException
        {
            String isoDate = jsonParser.readValueAs(String.class);
            return ZonedDateTime.parse(isoDate, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
    }

    public static class ApiCallRcEntry implements ApiCallRc.RcEntry
    {
        @JsonProperty("dateTime")
        private final ZonedDateTime createdAt;
        private @Nullable Map<String, String> objRefs = new HashMap<>();
        private @Nullable String message;
        private @Nullable String cause;
        private @Nullable String correction;
        private @Nullable String details;
        private @Nullable Set<String> errorIds = new TreeSet<>();
        private long returnCode = 0;
        private boolean skipErrorReport = false;
        private boolean appendObjectDescriptionToDetails = true;

        private static final Map<Long, LinstorObj> POSSIBLE_OBJS = Map.ofEntries(
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_SCHEDULE, LinstorObj.SCHEDULE),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_EXT_FILES, LinstorObj.EXT_FILES),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_PHYSICAL_DEVICE, LinstorObj.PHYSICAL_DEVICE),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_VLM_GRP, LinstorObj.VLM_GRP),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_RSC_GRP, LinstorObj.RSC_GRP),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_KVS, LinstorObj.KVS),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_NODE, LinstorObj.NODE),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_RSC_DFN, LinstorObj.RSC_DFN),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_RSC, LinstorObj.RSC),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_VLM_DFN, LinstorObj.VLM_DFN),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_VLM, LinstorObj.VLM),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_NODE_CONN, LinstorObj.NODE_CONN),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_RSC_CONN, LinstorObj.RSC_CONN),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_VLM_CONN, LinstorObj.VLM_CONN),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_NET_IF, LinstorObj.NET_IF),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_STOR_POOL_DFN, LinstorObj.STOR_POOL_DFN),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_STOR_POOL, LinstorObj.STOR_POOL),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_CTRL_CONF, LinstorObj.CTRL_CONF),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_SNAPSHOT, LinstorObj.SNAPSHOT),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_BACKUP, LinstorObj.BACKUP),
            new AbstractMap.SimpleEntry<>(ApiConsts.MASK_REMOTE, LinstorObj.REMOTE)
        );

        public ApiCallRcEntry()
        {
            createdAt = ZonedDateTime.now();
        }

        @Override
        @JsonSerialize(using = ZonedDTToString.class)
        @JsonDeserialize(using = StringToZonedDT.class)
        public ZonedDateTime getDateTime()
        {
            return createdAt;
        }

        @Override
        @JsonIgnore
        public Severity getSeverity()
        {
            Severity sev = Severity.INFO;
            if ((returnCode & ApiConsts.MASK_BITS_TYPE) == ApiConsts.MASK_ERROR)
            {
                sev = Severity.ERROR;
            }
            else if ((returnCode & ApiConsts.MASK_BITS_TYPE) == ApiConsts.MASK_WARN)
            {
                sev = Severity.WARNING;
            }

            return sev;
        }

        @Override
        @JsonIgnore
        public Action getAction()
        {
            Action action = Action.UNKNOWN;
            if ((returnCode & ApiConsts.MASK_BITS_OP) == ApiConsts.MASK_CRT)
            {
                action = Action.CREATE;
            }
            else if ((returnCode & ApiConsts.MASK_BITS_OP) == ApiConsts.MASK_MOD)
            {
                action = Action.MODIFY;
            }
            else if ((returnCode & ApiConsts.MASK_BITS_OP) == ApiConsts.MASK_DEL)
            {
                action = Action.DELETE;
            }
            return action;
        }

        @Override
        @JsonIgnore
        public Set<LinstorObj> getObjects()
        {
            Set<LinstorObj> objs = new HashSet<>();
            for (var entries : POSSIBLE_OBJS.entrySet())
            {
                if ((returnCode & ApiConsts.MASK_BITS_OBJ) == entries.getKey())
                {
                    objs.add(entries.getValue());
                }
            }
            return objs;
        }

        @Override
        @JsonIgnore
        public long getErrorCode()
        {
            return returnCode & ApiConsts.MASK_BITS_CODE;
        }

        public ApiCallRcEntry setReturnCode(long returnCodeRef)
        {
            returnCode = returnCodeRef;
            return this;
        }

        public ApiCallRcEntry setReturnCodeBit(long bitMask)
        {
            returnCode |= bitMask;
            return this;
        }

        public ApiCallRcEntry setMessage(@Nullable String messageRef)
        {
            message = messageRef;
            return this;
        }

        public ApiCallRcEntry setCause(@Nullable String causeRef)
        {
            cause = causeRef;
            return this;
        }

        public ApiCallRcEntry setCorrection(@Nullable String correctionRef)
        {
            correction = correctionRef;
            return this;
        }

        public ApiCallRcEntry setDetails(@Nullable String detailsRef)
        {
            details = detailsRef;
            return this;
        }

        public ApiCallRcEntry putObjRef(String key, String value)
        {
            objRefs.put(key, value);
            return this;
        }

        public ApiCallRcEntry putAllObjRef(Map<String, String> map)
        {
            objRefs.putAll(map);
            return this;
        }

        public ApiCallRcEntry addErrorId(String errorId)
        {
            errorIds.add(errorId);
            return this;
        }

        public ApiCallRcEntry addAllErrorIds(Set<String> errorIdsRef)
        {
            errorIds.addAll(errorIdsRef);
            return this;
        }

        public ApiCallRcEntry setSkipErrorReport(boolean skip)
        {
            skipErrorReport = skip;
            return this;
        }

        public ApiCallRcEntry setAppendObjectDescriptionToDetails(boolean appendObjectDescriptionToDetailsRef)
        {
            appendObjectDescriptionToDetails = appendObjectDescriptionToDetailsRef;
            return this;
        }

        @Override
        public long getReturnCode()
        {
            return returnCode;
        }

        @Override
        public @Nullable Map<String, String> getObjRefs()
        {
            return objRefs;
        }

        @Override
        public @Nullable String getMessage()
        {
            return message;
        }

        @Override
        public @Nullable String getCause()
        {
            return cause;
        }

        @Override
        public @Nullable String getCorrection()
        {
            return correction;
        }

        @Override
        public @Nullable String getDetails()
        {
            return details;
        }

        @Override
        public @Nullable Set<String> getErrorIds()
        {
            return errorIds;
        }

        @Override
        @JsonIgnore
        public boolean appendObjectDescrptionToDetails()
        {
            return appendObjectDescriptionToDetails;
        }

        @Override
        @JsonIgnore
        public boolean isError()
        {
            return (returnCode & ApiConsts.MASK_ERROR) == ApiConsts.MASK_ERROR;
        }

        @Override
        @JsonGetter
        public boolean skipErrorReport()
        {
            return !isError() || skipErrorReport;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            ApiRcUtils.appendReadableRetCode(sb, returnCode);

            return "ApiCallRcEntry{" +
                "returnCode=" + sb +
                ", objRefs=" + objRefs +
                ", message='" + message + '\'' +
                ", cause='" + cause + '\'' +
                ", correction='" + correction + '\'' +
                ", details='" + details + '\'' +
                ", errorIds=" + errorIds +
                ", skipErrorReport=" + skipErrorReport +
                ", dt=" + createdAt +
                '}';
        }
    }

    public static class EntryBuilder
    {
        private final long returnCode;

        private final String message;

        private @Nullable String cause;

        private @Nullable String correction;

        private @Nullable String details;
        private boolean skipErrorReport = false;
        private boolean appendObjectDescriptionToDetails = true;

        private final Map<String, String> objRefs = new TreeMap<>();

        private final Set<String> errorIds = new TreeSet<>();

        private EntryBuilder(long returnCodeRef, String messageRef)
        {
            returnCode = returnCodeRef;
            message = messageRef;
        }

        public EntryBuilder setCause(@Nullable String causeRef)
        {
            cause = causeRef;
            return this;
        }

        public EntryBuilder setCorrection(@Nullable String correctionRef)
        {
            correction = correctionRef;
            return this;
        }

        public EntryBuilder setDetails(@Nullable String detailsRef)
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
            if (errorId != null)
            {
                errorIds.add(errorId);
            }
            return this;
        }

        public EntryBuilder addAllErrorIds(Collection<String> errorIdsRef)
        {
            errorIds.addAll(errorIdsRef);
            return this;
        }

        public EntryBuilder setSkipErrorReport(boolean skip)
        {
            skipErrorReport = skip;
            return this;
        }

        public EntryBuilder setAppendObjectDescriptionToDetails(boolean appendRef)
        {
            appendObjectDescriptionToDetails = appendRef;
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
            entry.setSkipErrorReport(skipErrorReport);
            entry.setAppendObjectDescriptionToDetails(appendObjectDescriptionToDetails);
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

    public static ApiCallRcEntry copyAndPrefixMessage(String prefix, RcEntry rcEntryRef)
    {
        ApiCallRcImpl.ApiCallRcEntry ret = new ApiCallRcEntry();
        ret.returnCode = rcEntryRef.getReturnCode();
        ret.message = prefix + rcEntryRef.getMessage();

        ret.cause = rcEntryRef.getCause();
        ret.correction = rcEntryRef.getCorrection();
        ret.details = rcEntryRef.getDetails();
        ret.skipErrorReport = rcEntryRef.skipErrorReport();
        ret.errorIds = new HashSet<>(rcEntryRef.getErrorIds());
        ret.objRefs = new HashMap<>(rcEntryRef.getObjRefs());

        return ret;
    }

    public static ApiCallRcImpl copyAndPrefix(String prefix, ApiCallRcImpl apiCallRcImpl)
    {
        ApiCallRcImpl ret = new ApiCallRcImpl();
        for (RcEntry rcEntry : apiCallRcImpl.entries)
        {
            ret.addEntry(copyAndPrefixMessage(prefix, rcEntry));
        }
        return ret;
    }

    @Override
    public int size()
    {
        return entries.size();
    }

    @Override
    @JsonIgnore
    public boolean isEmpty()
    {
        return entries.isEmpty();
    }

    @Override
    public void replaceAll(UnaryOperator<RcEntry> operator)
    {
        throw new ImplementationError("ApiCallRc replaceAll not allowed");
    }

    @Override
    public void sort(Comparator<? super RcEntry> comp)
    {
        throw new ImplementationError("ApiCallRc sort not allowed");
    }

    @Override
    public Spliterator<RcEntry> spliterator()
    {
        return entries.spliterator();
    }

    @Override
    public boolean contains(Object obj)
    {
        return entries.contains(obj);
    }

    @Override
    public Iterator<RcEntry> iterator()
    {
        return entries.iterator();
    }

    @Override
    public Object[] toArray()
    {
        return entries.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts)
    {
        return entries.toArray(ts);
    }

    @Override
    public boolean add(RcEntry rcEntry)
    {
        return entries.add(rcEntry);
    }

    @Override
    public boolean remove(Object obj)
    {
        // return entries.remove(o);
        throw new ImplementationError("ApiCallRc remove not allowed");
    }

    @Override
    public boolean containsAll(Collection<?> collection)
    {
        return new HashSet<>(entries).containsAll(collection);
    }

    @Override
    public boolean addAll(Collection<? extends RcEntry> collection)
    {
        return entries.addAll(collection);
    }

    @Override
    public boolean addAll(int index, Collection<? extends RcEntry> collection)
    {
        return entries.addAll(index, collection);
    }

    @Override
    public boolean removeAll(Collection<?> collection)
    {
        // return entries.removeAll(collection);
        throw new ImplementationError("ApiCallRc removeAll not allowed");
    }

    @Override
    public boolean retainAll(Collection<?> collection)
    {
        // return entries.retainAll(collection);
        throw new ImplementationError("ApiCallRc retainAll not allowed");
    }

    @Override
    public void clear()
    {
        // entries.clear();
        throw new ImplementationError("ApiCallRc retainAll not allowed");
    }

    @Override
    public RcEntry get(int index)
    {
        return entries.get(index);
    }

    @Override
    public RcEntry set(int index, RcEntry rcEntry)
    {
        // return entries.set(index, rcEntry);
        throw new ImplementationError("ApiCallRc set not allowed");
    }

    @Override
    public void add(int index, RcEntry rcEntry)
    {
        entries.add(index, rcEntry);
    }

    @Override
    public RcEntry remove(int index)
    {
        // return entries.remove(index);
        throw new ImplementationError("ApiCallRc remove not allowed");
    }

    @Override
    public int indexOf(Object obj)
    {
        return entries.indexOf(obj);
    }

    @Override
    public int lastIndexOf(Object obj)
    {
        return entries.lastIndexOf(obj);
    }

    @Override
    public ListIterator<RcEntry> listIterator()
    {
        return entries.listIterator();
    }

    @Override
    public ListIterator<RcEntry> listIterator(int index)
    {
        return entries.listIterator(index);
    }

    @Override
    public List<RcEntry> subList(int fromIndex, int toIndex)
    {
        return entries.subList(fromIndex, toIndex);
    }
}
