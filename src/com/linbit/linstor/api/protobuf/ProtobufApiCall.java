package com.linbit.linstor.api.protobuf;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

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
}
