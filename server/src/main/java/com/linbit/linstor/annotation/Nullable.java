package com.linbit.linstor.annotation;

import javax.annotation.meta.TypeQualifierNickname;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@TypeQualifierNickname
@Retention(RetentionPolicy.RUNTIME)
@Target(
    {
        ElementType.TYPE_PARAMETER,
        ElementType.FIELD,
        ElementType.LOCAL_VARIABLE,
        ElementType.METHOD,
        ElementType.PARAMETER,
        ElementType.TYPE,
        ElementType.TYPE_USE
    }
)
@javax.annotation.CheckForNull
public @interface Nullable {

}
