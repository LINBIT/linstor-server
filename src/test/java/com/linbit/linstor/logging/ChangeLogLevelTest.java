package com.linbit.linstor.logging;

import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.TestAccessContextProvider;

import java.nio.file.Paths;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.event.Level;

import static org.junit.Assert.fail;

public class ChangeLogLevelTest
{
    private StdErrorReporter reporter;

    public ChangeLogLevelTest() throws AccessDeniedException
    {
        reporter = new StdErrorReporter(
            "test",
            Paths.get("."),
            true,
            "node",
            "INFO",
            "DEBUG",
            () -> TestAccessContextProvider.INIT_CTX
        );
        reporter = Mockito.spy(reporter);
        Mockito.doAnswer(invoc ->
        {
            fail();
            return null;
        })
            .when(reporter)
            .logError(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testChangeLogLevel() throws Exception
    {
        reporter.setLogLevel(TestAccessContextProvider.INIT_CTX, Level.DEBUG, Level.ERROR);
        reporter.setLogLevel(TestAccessContextProvider.INIT_CTX, Level.INFO, null);
        reporter.setLogLevel(TestAccessContextProvider.INIT_CTX, null, Level.TRACE);
        reporter.setLogLevel(TestAccessContextProvider.INIT_CTX, null, null);
    }
}
