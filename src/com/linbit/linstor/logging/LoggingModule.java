package com.linbit.linstor.logging;

import com.google.inject.AbstractModule;

public class LoggingModule extends AbstractModule
{
    private final ErrorReporter errorReporter;

    public LoggingModule(ErrorReporter errorReporter)
    {
        this.errorReporter = errorReporter;
    }

    @Override
    protected void configure()
    {
        bind(ErrorReporter.class).toInstance(errorReporter);
    }
}
