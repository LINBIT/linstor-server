package com.linbit.linstor.utils;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class NameShortenerModule extends AbstractModule
{
    private static final String OPENFLEX_PROP_NAMESPACE = "OpenFlex";
    private static final String OPENFLEX_PROP_KEY = "shortName";
    private static final int OPENFLEX_MAX_LEN = 32;
    private static final String OPENFLEX_DELIMITER = "_";
    private static final String OPENFLEX_VALID_CHARS_REGEX = "a-zA-Z0-9_";

    public static final String EXOS_PROP_NAMESPACE = "Exos";
    public static final String EXOS_PROP_KEY = "shortName";
    private static final int EXOS_MAX_LEN = 32;
    private static final String EXOS_DELIMITER = "_";
    private static final String EXOS_VALID_CHARS_REGEX = "a-zA-Z0-9_";

    @Provides
    @Singleton
    @Named(NameShortener.OPENFLEX)
    public NameShortener createOpenFlexNameShortener(
        @SystemContext AccessContext sysCtx
    )
    {
        return new NameShortener(
            OPENFLEX_PROP_NAMESPACE,
            OPENFLEX_PROP_KEY,
            OPENFLEX_MAX_LEN,
            sysCtx,
            OPENFLEX_DELIMITER,
            OPENFLEX_VALID_CHARS_REGEX
        );
    }

    @Provides
    @Singleton
    @Named(NameShortener.EXOS)
    public NameShortener createExosNameShortener(
        @SystemContext AccessContext sysCtx
    )
    {
        return new NameShortener(
            EXOS_PROP_NAMESPACE,
            EXOS_PROP_KEY,
            EXOS_MAX_LEN,
            sysCtx,
            EXOS_DELIMITER,
            EXOS_VALID_CHARS_REGEX
        );
    }
}
