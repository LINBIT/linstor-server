package com.linbit.linstor.api.protobuf;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(TYPE)
public @interface ProtobufApiCall
{
    /**
     * Returns the name of the API call
     *
     * @return Name of the API call
     */
    String name();

    /**
     * Returns the description of the API call's function
     *
     * @return Description of the API call's function
     */
    String description();

    /**
     * Indicates whether the API requires an authenticated peer
     *
     * @return True if the API may only be called by authenticated peers, false if the API can be called
     *         by the PUBLIC identity
     */
    boolean requiresAuth() default true;

    /**
     * Indicates whether the API requires a database transaction. That is, whether the API writes to the persistent
     * data structures. Read-only APIs do not require a transaction, because data is read from the in-memory cached
     * objects and read consistency is handled by locks rather than by transactions.
     * <p>
     * Only applies to {@link com.linbit.linstor.api.ApiCall ApiCall}.
     *
     * @return True if the API requires a database transaction.
     */
    boolean transactional() default true;
}
