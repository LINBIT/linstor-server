package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.security.LdapAuthentication;
import com.linbit.linstor.testutils.EmptyErrorReporter;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;

import java.time.Duration;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/** Values passed through offWorker are delivered on the RestResponse scheduler, not the emitting thread. */
public class RequestHelperOffWorkerTest
{
    private Scheduler restResponse;
    private Scheduler emitter;
    private RequestHelper helper;

    @Before
    public void setUp()
    {
        restResponse = Schedulers.newBoundedElastic(2, 8, "RestResponse");
        emitter = Schedulers.newSingle("InOrderWorker");
        helper = new RequestHelper(
            new EmptyErrorReporter(),
            mock(LinStorScope.class),
            mock(TransactionMgrGenerator.class),
            mock(LdapAuthentication.class),
            mock(CtrlConfig.class),
            restResponse
        );
    }

    @After
    public void tearDown()
    {
        restResponse.dispose();
        emitter.dispose();
    }

    @Test
    public void renderRunsOnRestResponseThread()
    {
        List<String> threads = helper
            .offWorker(Flux.just(1, 2, 3).subscribeOn(emitter))
            .map(ignored -> Thread.currentThread().getName())   // stands in for the JSON render
            .collectList()
            .block(Duration.ofSeconds(5));
        assertEquals(3, threads.size());
        for (String name : threads)
        {
            assertTrue("render ran on " + name, name.startsWith("RestResponse"));
        }
    }
}
