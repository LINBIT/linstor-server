package com.linbit.linstor.core;

import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.testutils.EmptyErrorReporter;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StltClientConfWriterTest
{
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private StltConfig stltCfg;
    private Path targetPath;

    @Before
    public void setUp() throws Exception
    {
        targetPath = tmpFolder.newFolder("run").toPath().resolve("linstor-client.conf");
        stltCfg = new StltConfig();
        stltCfg.setClientConfFile(targetPath.toString());
    }

    @Test
    public void writesControllerAddress() throws Exception
    {
        newWriter().update(peerWithAddress("10.43.0.7"));

        assertTrue(
            Files.readString(targetPath).contains(System.lineSeparator() + "controllers = linstor://10.43.0.7")
        );
        assertEquals("rw-r--r--", PosixFilePermissions.toString(Files.getPosixFilePermissions(targetPath)));
    }

    @Test
    public void bracketsIpv6Address() throws Exception
    {
        newWriter().update(peerWithAddress("2001:db8::1"));

        assertTrue(Files.readString(targetPath).contains("controllers = linstor://[2001:db8:0:0:0:0:0:1]"));
    }

    @Test
    public void rewritesOnlyWhenTheAddressChanges() throws Exception
    {
        StltClientConfWriter writer = newWriter();

        writer.update(peerWithAddress("10.43.0.7"));
        long firstWrite = Files.getLastModifiedTime(targetPath).toMillis();

        writer.update(peerWithAddress("10.43.0.7"));
        assertEquals(firstWrite, Files.getLastModifiedTime(targetPath).toMillis());

        writer.update(peerWithAddress("10.43.0.8"));
        assertTrue(Files.readString(targetPath).contains("controllers = linstor://10.43.0.8"));
    }

    @Test
    public void skipsOfflinePeer() throws Exception
    {
        Peer offlinePeer = Mockito.mock(Peer.class);
        Mockito.when(offlinePeer.getHostAddr()).thenReturn(null);

        newWriter().update(offlinePeer);

        assertFalse(Files.exists(targetPath));
    }

    @Test
    public void skipsLinkLocalAddress() throws Exception
    {
        newWriter().update(peerWithAddress("169.254.1.1"));

        assertFalse(Files.exists(targetPath));
    }

    @Test
    public void skipsWhenTheRuntimeDirectoryIsMissing() throws Exception
    {
        Path missingDirPath = tmpFolder.getRoot().toPath().resolve("absent").resolve("linstor-client.conf");
        stltCfg.setClientConfFile(missingDirPath.toString());

        newWriter().update(peerWithAddress("10.43.0.7"));

        assertFalse(Files.exists(missingDirPath));
        assertFalse(Files.exists(missingDirPath.getParent()));
    }

    @Test
    public void skipsWhenDisabled() throws Exception
    {
        stltCfg.setClientConfFile("");

        newWriter().update(peerWithAddress("10.43.0.7"));

        assertFalse(Files.exists(targetPath));
    }

    private StltClientConfWriter newWriter()
    {
        return new StltClientConfWriter(new EmptyErrorReporter(), stltCfg);
    }

    private Peer peerWithAddress(String addressRef)
    {
        Peer peer = Mockito.mock(Peer.class);
        Mockito.when(peer.getHostAddr()).thenReturn(new InetSocketAddress(addressRef, 43210));
        return peer;
    }
}
