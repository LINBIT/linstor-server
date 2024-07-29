package com.linbit.linstor.api;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Return codes of an API call
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@JsonDeserialize(as = ApiCallRcImpl.class)
public interface ApiCallRc extends List<ApiCallRc.RcEntry>
{
    enum Severity {
        INFO,
        WARNING,
        ERROR,
    }

    enum LinstorObj
    {
        SCHEDULE,
        EXT_FILES,
        PHYSICAL_DEVICE,
        VLM_GRP,
        RSC_GRP,
        KVS,
        NODE,
        RSC_DFN,
        VLM_DFN,
        RSC,
        VLM,
        NODE_CONN,
        RSC_CONN,
        VLM_CONN,
        NET_IF,
        STOR_POOL_DFN,
        STOR_POOL,
        CTRL_CONF,
        SNAPSHOT,
        BACKUP,
        REMOTE,
    }

    enum Action {
        CREATE,
        MODIFY,
        DELETE,
        UNKNOWN,
    }

    /**
     * Checks if any of the rcEntries is an error.
     * @return true if ApiCallRc has error entries.
     */
    boolean hasErrors();

    /**
     * Returns if all RcEntries skip error report.
     * @return true if all skip error report.
     */
    default boolean allSkipErrorReport()
    {
        return stream().filter(RcEntry::isError).allMatch(RcEntry::skipErrorReport);
    }

    /**
     * Return code entry
     */
    @JsonDeserialize(as = ApiCallRcEntry.class)
    interface RcEntry
    {
        /**
         * Numeric return code describing the result of an operation
         */
        long getReturnCode();

        Severity getSeverity();

        Action getAction();

        Set<LinstorObj> getObjects();

        long getErrorCode();

        /**
         * Object references describing which object the return code refers to.

         * The key specifies WHAT the object is, and the value specifies
         * WHICH one of a list of objects is referenced.
         * E.g., for a Resource that has been assigned to a node,
         * there may be two object references, one with a key of "ResourceDefinition"
         * and a value stating the name of the ResourceDefinition for the resource,
         * and another one with a key of "Node" and a value specifying the name
         * of the Node that the Resource has been assigned to
         *
         * @return Map of object references
         */
        @Nullable
        Map<String, String> getObjRefs();

        /**
         * @return Reply message
         */
        @Nullable
        String getMessage();

        /**
         * @return Cause information
         */
        @Nullable
        String getCause();

        /**
         * @return Correction hint
         */
        @Nullable
        String getCorrection();

        /**
         * @return Details information
         */
        @Nullable
        String getDetails();

        /**
         * If set to true, the details-section will be appended with the object description
         */
        boolean appendObjectDescrptionToDetails();

        /**
         * @return Date the apicallrc was created
         */
        ZonedDateTime getDateTime();

        /**
         * Error ids linked to this apicallrc.
         * @return Set of error ids
         */
        Set<String> getErrorIds();

        /**
         * True if the RcEntry is an error.
         * @return True if the RcEntry is an error.
         */
        boolean isError();

        boolean skipErrorReport();
    }
}
