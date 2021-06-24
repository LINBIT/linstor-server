package com.linbit.linstor.api;

import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;

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
public interface ApiCallRc
{

    // List of return codes
    List<RcEntry> getEntries();

    boolean isEmpty();

    /**
     * Checks if any of the rcEntries is an error.
     * @return true if ApiCallRc has error entries.
     */
    boolean hasErrors();

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

        /**
         * Object references describing which object the return code refers to.
         *
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
        Map<String, String> getObjRefs();

        /**
         * @return Reply message
         */
        String getMessage();

        /**
         * @return Cause information
         */
        String getCause();

        /**
         * @return Correction hint
         */
        String getCorrection();

        /**
         * @return Details information
         */
        String getDetails();

        /**
         * Error ids linked to this apicallrc.
         * @return List of error ids
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
