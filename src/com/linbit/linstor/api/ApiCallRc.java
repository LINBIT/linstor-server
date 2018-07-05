package com.linbit.linstor.api;

import java.util.List;
import java.util.Map;

/**
 * Return codes of an API call
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ApiCallRc
{

    // List of return codes
    List<RcEntry> getEntries();

    /**
     * Return code entry
     */
    public interface RcEntry
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
    }
}
