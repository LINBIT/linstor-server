package com.linbit.drbdmanage.api;

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
     *
     * Format strings and variables
     *
     * A format string can include the keys of variables that are included
     * in the return codes. Such a variable key must be prefixed by
     * a dollar sign and enclosed in curly braces (e.g., "${key}").
     * The backslash is reserved as an escape character.
     * To include a literal backslash in a message, use double backslashes.
     * To escape the special meaning of "${" as the initiator of a variable
     * in the message, escape the dollar sign with a backslash (e.g. "\${").
     */
    public static interface RcEntry
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
         * The client-localizable format string of the output message
         *
         * See the description of format strings and variables in the interface's documentation
         *
         * @return Reply message format string
         */
        String getMessageFormat();

        /**
         * The client-localizable format string of the cause information of an error
         *
         * See the description of format strings and variables in the interface's documentation
         *
         * @return Cause information format string
         */
        String getCauseFormat();

        /**
         * The client-localizable format string of the correction hint of an error
         *
         * See the description of format strings and variables in the interface's documentation
         *
         * @return Correction hint format string
         */
        String getCorrectionFormat();

        /**
         * The client-localizable format string of additional details information
         *
         * See the description of format strings and variables in the interface's documentation
         *
         * @return Details information format string
         */
        String getDetailsFormat();

        /**
         * The keys and values for variable replacement in format strings
         *
         * @return Key/value map for variable replacement in format strings
         */
        Map<String, String> getVariables();
    }
}
