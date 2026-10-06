package com.linbit.fsevent;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.Platform;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.layer.storage.AbsStorageProvider.AbsStorageProviderInit;
import com.linbit.linstor.layer.storage.utils.BlockSizeInfo;
import com.linbit.linstor.layer.storage.zfs.ZfsThinProvider;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

public class ProbeVolumeDeviceWaitTest
{
    private static final StorPoolName MOCKED_SP_NAME;

    static
    {
        try
        {
            MOCKED_SP_NAME = new StorPoolName("data");
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Rule
    public final TemporaryFolder directory = new TemporaryFolder();

    @Rule
    public final Timeout timeout = Timeout.seconds(15);

    private FileSystemWatch watch;
    private Path device;
    private Path link;
    private StorPool pool;
    private ProbeProvider provider;
    private ErrorReporter reporter;
    private ScheduledExecutorService executor;
    private MockedStatic<BlockSizeInfo> blockSizes;

    @Before
    public void setUp() throws Exception
    {
        assumeTrue(Platform.isLinux());
        device = directory.newFile("zd-test").toPath();
        link = directory.getRoot().toPath().resolve("probe");
        executor = Executors.newSingleThreadScheduledExecutor();
        reporter = mock(ErrorReporter.class);
        watch = new FileSystemWatch(reporter);
        watch.start();
        pool = mock(StorPool.class);
        when(pool.getName()).thenReturn(MOCKED_SP_NAME);
        when(pool.getVolumes()).thenReturn(Collections.emptyList());
        when(pool.getProps()).thenReturn(mock(Props.class));
        Node localNode = mock(Node.class);
        when(pool.getNode()).thenReturn(localNode);
        ControllerPeerConnector ctrlPeerConnector = mock(ControllerPeerConnector.class);
        when(ctrlPeerConnector.getLocalNode()).thenReturn(localNode);
        provider = new ProbeProvider(new AbsStorageProviderInit(
            reporter, null, null, null, null, null, null, null, null, watch, null, null, null, ctrlPeerConnector
        ));
        blockSizes = mockStatic(BlockSizeInfo.class);
        blockSizes.when(() -> BlockSizeInfo.getPhysicalBlockSize(device)).thenAnswer(invocation ->
        {
            assertTrue("Probe must still exist while its properties are read", Files.exists(link));
            return 4096L;
        });
        blockSizes.when(() -> BlockSizeInfo.getOptimalIoSize(device)).thenReturn(33554432L);
        blockSizes.when(() -> BlockSizeInfo.getDiscardGranularity(device)).thenReturn(16384L);
    }

    @After
    public void tearDown() throws Exception
    {
        if (blockSizes != null)
        {
            blockSizes.close();
        }
        if (executor != null)
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
        if (watch != null)
        {
            watch.shutdown(false);
            watch.awaitShutdown(2000);
            watch.cancelAllWatchKeys();
        }
    }

    @Test
    public void delayedDevicePublishesPropertiesBeforeCleanup() throws Exception
    {
        provider.delayMillis = 50;
        assertPublished(probe());
        assertCleaned(1);
        assertEquals(0, warningCount());
    }

    @Test
    public void existingProbeDevicePublishesProperties() throws Exception
    {
        assertPublished(probe());
        assertCleaned(1);
    }

    @Test
    public void missingDeviceTimesOutAndCleansEveryAttempt() throws Exception
    {
        provider.delayMillis = -1;
        provider.timeoutMillis = 20;
        assertTrue(probe().isEmpty());
        assertCleaned(3);
        assertEquals(1, warningCount());
    }

    @Test
    public void failedCreationStopsRetriesWithoutCleanup() throws Exception
    {
        provider.failCreateTimes = Integer.MAX_VALUE;
        assertTrue(probe().isEmpty());
        assertEquals(1, provider.createCalls);
        assertEquals(0, provider.deleteCalls);
        assertEquals(1, warningCount());
    }

    @Test
    public void creationFailureAfterTimedOutAttemptStopsRetries() throws Exception
    {
        provider.delayMillis = -1;
        provider.timeoutMillis = 20;
        provider.failCreateOnCall = 2;
        assertTrue(probe().isEmpty());
        assertEquals(2, provider.createCalls);
        assertEquals(1, provider.deleteCalls);
        assertEquals(1, warningCount());
    }

    @Test
    public void nullProbePathDoesNotPublishProperties() throws Exception
    {
        provider.nullPath = true;
        assertTrue(probe().isEmpty());
        assertCleaned(3);
        assertEquals(1, warningCount());
    }

    @Test
    public void nullFirstPathDoesNotSkipUsableVolume() throws Exception
    {
        Files.createSymbolicLink(link, device);
        doReturn(List.of(volume(null), volume(link))).when(pool).getVolumes();
        assertPublished(probe());
        assertEquals(0, provider.createCalls);
        assertEquals(0, provider.deleteCalls);
    }

    @Test
    public void missingFirstPathDoesNotSkipUsableVolume() throws Exception
    {
        Files.createSymbolicLink(link, device);
        doReturn(List.of(volume(link.resolveSibling("missing")), volume(link))).when(pool).getVolumes();
        assertPublished(probe());
        assertEquals(0, provider.createCalls);
        assertEquals(0, provider.deleteCalls);
    }

    @Test
    public void allNullPathsFallBackToProbe() throws Exception
    {
        doReturn(List.of(volume(null))).when(pool).getVolumes();
        provider.delayMillis = 50;
        assertPublished(probe());
        assertCleaned(1);
    }

    @Test
    public void allMissingPathsFallBackToProbe() throws Exception
    {
        doReturn(List.of(
            volume(link.resolveSibling("missing-a")), volume(link.resolveSibling("missing-b"))
        )).when(pool).getVolumes();
        provider.delayMillis = 50;
        assertPublished(probe());
        assertCleaned(1);
    }

    @Test
    public void cleanupFailureAfterSuccessIsReported() throws Exception
    {
        provider.failDelete = true;
        assertPublished(probe());
        assertEquals(1, provider.createCalls);
        assertEquals(1, provider.deleteCalls);
        assertEquals(1, warningCount());
    }

    @Test
    public void cleanupFailurePreventsAnotherCreationAttempt() throws Exception
    {
        provider.delayMillis = -1;
        provider.timeoutMillis = 20;
        provider.failDelete = true;
        assertTrue(probe().isEmpty());
        assertEquals(1, provider.createCalls);
        assertEquals(1, provider.deleteCalls);
        assertEquals(2, warningCount());
    }

    @Test
    public void interruptedDeviceWaitCleansUpAndPreservesInterrupt() throws Exception
    {
        provider.delayMillis = -1;
        Thread callingThread = Thread.currentThread();
        ScheduledFuture<?> interruptableTask = executor.schedule(callingThread::interrupt, 50, TimeUnit.MILLISECONDS);
        try
        {
            assertTrue(probe().isEmpty());
            assertTrue(callingThread.isInterrupted());
            assertCleaned(1);
            assertEquals(1, warningCount());
        }
        finally
        {
            interruptableTask.cancel(false);
            Thread.interrupted();
        }
    }

    @Test
    public void interruptedRetryDelayDoesNotCreateAnotherProbe() throws Exception
    {
        provider.delayMillis = -1;
        provider.timeoutMillis = 200;
        Thread callingThread = Thread.currentThread();
        ScheduledFuture<?> interruptableTask = executor.schedule(callingThread::interrupt, 300, TimeUnit.MILLISECONDS);
        try
        {
            assertTrue(probe().isEmpty());
            assertTrue(callingThread.isInterrupted());
            assertCleaned(1);
            assertEquals(1, warningCount());
        }
        finally
        {
            interruptableTask.cancel(false);
            Thread.interrupted();
        }
    }

    @Test
    public void interruptedCleanupPreservesInterruptAndStopsRetries() throws Exception
    {
        provider.delayMillis = -1;
        provider.timeoutMillis = 20;
        provider.interruptDelete = true;
        try
        {
            assertTrue(probe().isEmpty());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, provider.createCalls);
            assertEquals(1, provider.deleteCalls);
        }
        finally
        {
            Thread.interrupted();
        }
    }

    @Test
    public void interruptedProbeStopsWithoutRetryDelay() throws Exception
    {
        blockSizes.when(() -> BlockSizeInfo.getPhysicalBlockSize(device)).thenAnswer(invocation ->
        {
            Thread.currentThread().interrupt();
            throw new IOException("probe read failed");
        });
        try
        {
            assertTrue(probe().isEmpty());
            assertTrue(Thread.currentThread().isInterrupted());
            assertCleaned(1);
            assertEquals(1, warningCount());
            // The probe failure itself must end the loop. Without the interrupt-flag check the loop would
            // only stop inside the retry sleep, and the reported reason would be the interrupted delay.
            assertEquals("probe read failed", lastWarningFinalArgument());
        }
        finally
        {
            Thread.interrupted();
        }
    }

    @Test
    public void concurrentProbesDoNotShareTheTemporaryVolume() throws Exception
    {
        provider.creationDelayMillis = 200;
        ExecutorService callers = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        Callable<LocalPropsChangePojo> call = () ->
        {
            ready.countDown();
            start.await();
            return probe();
        };
        try
        {
            Future<LocalPropsChangePojo> first = callers.submit(call);
            Future<LocalPropsChangePojo> second = callers.submit(call);
            assertTrue(ready.await(2, TimeUnit.SECONDS));
            start.countDown();
            assertFalse(first.get(10, TimeUnit.SECONDS).isEmpty());
            assertFalse(second.get(10, TimeUnit.SECONDS).isEmpty());
            assertEquals(1, provider.peakActiveProbes.get());
            assertCleaned(2);
        }
        finally
        {
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    private LocalPropsChangePojo probe()
    {
        LocalPropsChangePojo changes = new LocalPropsChangePojo();
        provider.updateBlockDeviceInfo(pool, changes);
        return changes;
    }

    private void assertPublished(LocalPropsChangePojo changesRef)
    {
        String namespace = StorageConstants.NAMESPACE_INTERNAL + '/';
        assertEquals(Map.of(
            namespace + StorageConstants.BLK_DEV_MIN_IO_SIZE, "4096",
            namespace + StorageConstants.BLK_DEV_OPT_IO_SIZE, "33554432",
            namespace + StorageConstants.BLK_DEV_DISC_GRAN, "16384"
        ), changesRef.changedStorPoolProps.get(MOCKED_SP_NAME));
    }

    private void assertCleaned(int attemptsRef)
    {
        assertEquals(attemptsRef, provider.createCalls);
        assertEquals(attemptsRef, provider.deleteCalls);
        assertFalse(Files.exists(link));
    }

    @SuppressWarnings("unchecked")
    private VlmProviderObject<Resource> volume(Path pathRef)
    {
        VlmProviderObject<Resource> volume = mock(VlmProviderObject.class);
        when(volume.getDevicePath()).thenReturn(pathRef == null ? null : pathRef.toString());
        return volume;
    }

    private long warningCount()
    {
        return mockingDetails(reporter).getInvocations().stream()
            .filter(invocation -> invocation.getMethod().getName().equals("logWarning"))
            .count();
    }

    private Object lastWarningFinalArgument()
    {
        return mockingDetails(reporter).getInvocations().stream()
            .filter(invocation -> invocation.getMethod().getName().equals("logWarning"))
            .reduce((first, second) -> second)
            .map(invocation -> invocation.getArguments()[invocation.getArguments().length - 1])
            .orElse(null);
    }

    private class ProbeProvider extends ZfsThinProvider
    {
        private long delayMillis;
        private long creationDelayMillis;
        private long timeoutMillis = 4000;
        private int failCreateTimes;
        private int failCreateOnCall;
        private boolean nullPath;
        private boolean failDelete;
        private boolean interruptDelete;
        private int createCalls;
        private int deleteCalls;
        private ScheduledFuture<?> create;
        private final AtomicInteger activeProbes = new AtomicInteger();
        private final AtomicInteger peakActiveProbes = new AtomicInteger();

        ProbeProvider(AbsStorageProviderInit initRef)
        {
            super(initRef);
        }

        @Override
        protected long getWaitTimeoutAfterCreate(StorPool ignored)
        {
            return timeoutMillis;
        }

        @Override
        public String createTmpProbeVlm(StorPool ignored) throws StorageException
        {
            peakActiveProbes.accumulateAndGet(activeProbes.incrementAndGet(), Math::max);
            createCalls++;
            if (creationDelayMillis > 0)
            {
                try
                {
                    Thread.sleep(creationDelayMillis);
                }
                catch (InterruptedException exc)
                {
                    throw new StorageException("Test probe creation interrupted", exc);
                }
            }
            if (failCreateTimes > 0)
            {
                failCreateTimes--;
                throw new StorageException("Probe creation failed");
            }
            if (failCreateOnCall == createCalls)
            {
                throw new StorageException("Probe creation failed");
            }
            if (nullPath)
            {
                return null;
            }
            if (delayMillis == 0)
            {
                try
                {
                    Files.createSymbolicLink(link, device);
                }
                catch (IOException exc)
                {
                    throw new StorageException("Test device creation failed", exc);
                }
            }
            else if (delayMillis > 0)
            {
                create = executor.schedule(() -> Files.createSymbolicLink(link, device),
                    delayMillis, TimeUnit.MILLISECONDS);
            }
            return link.toString();
        }

        @Override
        public void deleteTmpProbeVlm(StorPool ignored) throws StorageException
        {
            deleteCalls++;
            if (interruptDelete)
            {
                throw new StorageException("Probe cleanup interrupted", new InterruptedException());
            }
            if (failDelete)
            {
                throw new StorageException("Probe cleanup failed");
            }
            try
            {
                if (create != null && !create.cancel(false))
                {
                    create.get();
                }
                Files.deleteIfExists(link);
                activeProbes.decrementAndGet();
            }
            catch (Exception exc)
            {
                throw new StorageException("Test device cleanup failed", exc);
            }
        }
    }
}
