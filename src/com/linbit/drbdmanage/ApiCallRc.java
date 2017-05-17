package com.linbit.drbdmanage;

import java.util.List;
import java.util.Map;

/**
 * Return codes of an API call
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ApiCallRc
{
    // Mask for return codes that describe an error
    public static final long MASK_ERROR = 0x8000000000000000L;
    
    // Mask for return codes that describe a warning
    public static final long MASK_WARN  = 0x4000000000000000L;

    // Mask for return codes that describe contain detail information
    // about the result of an operation
    public static final long MASK_INFO  = 0x2000000000000000L;

    // List of return codes
    List<RcEntry> getEntries();

    public static interface RcEntry
    {
        // Numeric return code describing the result of an operation
        long getReturnCode();

        // Object references describing which object the return code
        // refers to.
        // The key specifies WHAT the object is, and the value specifies
        // WHICH one of a list of objects is referenced.
        // E.g., for a Resource that has been assigned to a node,
        // there may be two object references, one with a key of "ResourceDefinition"
        // and a value stating the name of the ResourceDefinition for the resource,
        // and another one with a key of "Node" and a value specifying the name
        // of the Node that the Resource has been assigned to
        Map<String, String> getObjRefs();

        // The client-localizable format string of the output message.
        // The format can include the keys of variables that are included
        // in the return codes. Such a variable key must be prefixed by
        // a dollar sign and enclosed in curly braces (e.g., "${key}").
        // The backslash is reserved as an escape character.
        // To include a literal backslash in a message, use double backslashes.
        // To escape the special meaning of "${" as the initiator of a variable
        // in the message, escape the dollar sign with a backslash (e.g. "\${").
        String getMessageFormat();

        // The keys and values for variable replacement in the format string
        // of the output message
        Map<String, String> getVariables();
    }
}
