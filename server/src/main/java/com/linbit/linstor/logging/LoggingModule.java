package com.linbit.linstor.logging;

import com.google.inject.AbstractModule;

public class LoggingModule extends AbstractModule
{
    private final StdErrorReporter errorReporter;

    public LoggingModule(StdErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    @Override
    protected void configure()
    {
        bind(ErrorReporter.class).toInstance(errorReporter);
    }
}
