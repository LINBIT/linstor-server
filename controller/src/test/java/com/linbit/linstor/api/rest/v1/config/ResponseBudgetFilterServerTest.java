package com.linbit.linstor.api.rest.v1.config;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.glassfish.grizzly.http.CompressionConfig;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * ResponseBudgetFilter against a real Grizzly + Jersey server: clients that send a request and never read can hold at
 * most the budget, answers beyond it get 503, and a stalled write returns its lease at the transport write timeout.
 */
public class ResponseBudgetFilterServerTest
{
    private static final int BODY_MIB = 4;
    private static final long MIB = ResponseBudget.MIB;
    private static final int WRITE_TIMEOUT_SEC = 4;
    private static final String BODY;

    static
    {
        char[] chars = new char[BODY_MIB * 1024 * 1024];
        Arrays.fill(chars, 'x');
        BODY = new String(chars);
    }

    /** The server side of the last request, to check from the test whether the server closed it. */
    private static final java.util.concurrent.atomic.AtomicReference<org.glassfish.grizzly.Connection<?>> LAST_CONN =
        new java.util.concurrent.atomic.AtomicReference<>();

    @Path("big")
    public static class BigResource
    {
        @GET
        @Produces(MediaType.APPLICATION_JSON)
        public String big(@jakarta.ws.rs.core.Context org.glassfish.grizzly.http.server.Request request)
        {
            LAST_CONN.set(request.getContext().getConnection());
            return BODY;
        }
    }

    private HttpServer server;
    private final List<Socket> sockets = new ArrayList<>();

    @After
    public void tearDown() throws IOException
    {
        for (Socket sock : sockets)
        {
            sock.close();
        }
        if (server != null)
        {
            server.shutdownNow();
        }
    }

    private int start(ResponseBudget budget) throws IOException
    {
        int port;
        try (ServerSocket probe = new ServerSocket(0))
        {
            port = probe.getLocalPort();
        }
        ResourceConfig cfg = new ResourceConfig(BigResource.class);
        cfg.register(new ResponseBudgetFilter(budget, null));
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create("http://127.0.0.1:" + port + "/"), cfg, false);
        for (NetworkListener listener : server.getListeners())
        {
            listener.getTransport().setWriteTimeout(WRITE_TIMEOUT_SEC, TimeUnit.SECONDS);
            // as GrizzlyHttpService.enableCompression
            CompressionConfig compression = listener.getCompressionConfig();
            compression.setCompressionMode(CompressionConfig.CompressionMode.ON);
            compression.setCompressibleMimeTypes("application/json");
        }
        ResponseBudgetFilter.limitPendingBytes(server);
        server.start();
        return port;
    }

    /** Sends the request, reads only the status line, and then never reads again. */
    private String requestAndStall(int port) throws IOException
    {
        return requestAndStall(port, "GET", "");
    }

    private String requestAndStall(int port, String method, String extraHeaders) throws IOException
    {
        Socket sock = connect(port, method, extraHeaders);
        return readLine(sock.getInputStream());
    }

    private Socket connect(int port, String method, String extraHeaders) throws IOException
    {
        Socket sock = new Socket();
        sockets.add(sock);
        sock.setReceiveBufferSize(4096);
        sock.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 5_000);
        sock.setSoTimeout(10_000);
        sock.getOutputStream().write(
            (method + " /big HTTP/1.1\r\nHost: test\r\nConnection: close\r\n" + extraHeaders + "\r\n")
                .getBytes(StandardCharsets.US_ASCII)
        );
        sock.getOutputStream().flush();
        return sock;
    }

    private static String readLine(InputStream in) throws IOException
    {
        StringBuilder line = new StringBuilder();
        int chr;
        while ((chr = in.read()) != -1 && chr != '\n')
        {
            line.append((char) chr);
        }
        return line.toString().trim();
    }

    private static void waitFor(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException
    {
        long end = System.currentTimeMillis() + timeoutMs;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < end)
        {
            Thread.sleep(50);
        }
    }

    @Test
    public void nonReadingClientsHoldAtMostTheGlobalBudget() throws Exception
    {
        // room for two 4-MiB answers, per-client limits out of the way
        ResponseBudget budget = new ResponseBudget(9 * MIB, 1000 * MIB, 1000);
        int port = start(budget);

        int ok = 0;
        int refused = 0;
        for (int idx = 0; idx < 6; idx++)
        {
            String status = requestAndStall(port);
            if (status.startsWith("HTTP/1.1 200"))
            {
                ok++;
            }
            else if (status.startsWith("HTTP/1.1 503"))
            {
                refused++;
            }
            assertTrue("budget exceeded: " + budget.getGlobalUsedBytes(), budget.getGlobalUsedBytes() <= 9 * MIB);
        }
        assertEquals(2, ok);
        assertEquals(4, refused);

        // the stalled writes still hold their leases (the body is not queued in one piece)
        Thread.sleep(500);
        assertEquals(2 * BODY_MIB * MIB, budget.getGlobalUsedBytes());
        assertEquals(java.util.Set.of("127.0.0.1"), budget.getClientKeys());

        // the transport write timeout ends the stalled writes and returns the leases
        waitFor(() -> budget.getGlobalUsedBytes() == 0, (WRITE_TIMEOUT_SEC + 8) * 1000L);
        assertEquals(0, budget.getGlobalUsedBytes());
        assertEquals("HTTP/1.1 200 OK", requestAndStall(port));
    }

    @Test
    public void perClientCountLimitsOneClient() throws Exception
    {
        ResponseBudget budget = new ResponseBudget(1000 * MIB, 1000 * MIB, 1);
        int port = start(budget);
        assertEquals("HTTP/1.1 200 OK", requestAndStall(port));
        assertTrue(requestAndStall(port).startsWith("HTTP/1.1 503"));
    }

    @Test
    public void closedClientReturnsItsLease() throws Exception
    {
        ResponseBudget budget = new ResponseBudget(1000 * MIB, 1000 * MIB, 1000);
        int port = start(budget);
        assertEquals("HTTP/1.1 200 OK", requestAndStall(port));
        assertEquals(BODY_MIB * MIB, budget.getGlobalUsedBytes());
        sockets.get(0).close();
        waitFor(() -> budget.getGlobalUsedBytes() == 0, (WRITE_TIMEOUT_SEC + 8) * 1000L);
        assertEquals(0, budget.getGlobalUsedBytes());
    }

    @Test
    public void gzipAnswerArrivesCompleteAndReturnsItsLease() throws Exception
    {
        ResponseBudget budget = new ResponseBudget(1000 * MIB, 1000 * MIB, 1000);
        int port = start(budget);
        Socket sock = connect(port, "GET", "Accept-Encoding: gzip\r\n");
        InputStream in = sock.getInputStream();
        assertEquals("HTTP/1.1 200 OK", readLine(in));
        boolean gzip = false;
        boolean chunked = false;
        String header = readLine(in);
        while (!header.isEmpty())
        {
            String lower = header.toLowerCase(java.util.Locale.ROOT);
            gzip |= lower.equals("content-encoding: gzip");
            chunked |= lower.equals("transfer-encoding: chunked");
            header = readLine(in);
        }
        assertTrue("expected a gzip answer", gzip);
        assertTrue("expected chunked framing", chunked);
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        while (true)
        {
            int size = Integer.parseInt(readLine(in).trim(), 16);
            if (size == 0)
            {
                break;
            }
            compressed.write(in.readNBytes(size));
            readLine(in); // CRLF after the chunk
        }
        try (InputStream unzip = new GZIPInputStream(new java.io.ByteArrayInputStream(compressed.toByteArray())))
        {
            assertEquals(BODY_MIB * MIB, unzip.readAllBytes().length);
        }
        waitFor(() -> budget.getGlobalUsedBytes() == 0, 5_000);
        assertEquals(0, budget.getGlobalUsedBytes());
    }

    @Test
    public void headTakesNoLease() throws Exception
    {
        ResponseBudget budget = new ResponseBudget(1000 * MIB, 1000 * MIB, 1);
        int port = start(budget);
        assertEquals("HTTP/1.1 200 OK", requestAndStall(port, "HEAD", ""));
        assertEquals(0, budget.getGlobalUsedBytes());
        assertEquals("HTTP/1.1 200 OK", requestAndStall(port));
    }

    @Test
    public void writeTimeoutClosesTheConnectionWithoutTheClientReading() throws Exception
    {
        ResponseBudget budget = new ResponseBudget(1000 * MIB, 1000 * MIB, 1000);
        int port = start(budget);
        long start = System.currentTimeMillis();
        assertEquals("HTTP/1.1 200 OK", requestAndStall(port));
        org.glassfish.grizzly.Connection<?> serverSide = LAST_CONN.get();
        assertTrue(serverSide != null && serverSide.isOpen());

        waitFor(() -> budget.getGlobalUsedBytes() == 0, (WRITE_TIMEOUT_SEC + 8) * 1000L);
        long releasedAfterMs = System.currentTimeMillis() - start;
        assertEquals(0, budget.getGlobalUsedBytes());
        // the write ended by the timeout, not by completing into the kernel's buffers
        assertTrue(
            "lease released after " + releasedAfterMs + " ms",
            releasedAfterMs >= (WRITE_TIMEOUT_SEC - 1) * 1000L
        );

        // the client still reads nothing; the server must drop the connection by itself. A graceful close
        // would wait for the queued bytes to drain, i.e. for a client that never reads.
        waitFor(() -> !serverSide.isOpen(), 3_000);
        assertTrue("server kept the timed-out connection open", !serverSide.isOpen());
    }

    @Test
    public void writeTimeoutResetsTheConnection() throws Exception
    {
        // after the write timeout the client sees a reset, not a FIN after the server's unread send buffer
        ResponseBudget budget = new ResponseBudget(1000 * MIB, 1000 * MIB, 1000);
        int port = start(budget);
        assertEquals("HTTP/1.1 200 OK", requestAndStall(port));
        Socket sock = sockets.get(0);
        org.glassfish.grizzly.Connection<?> serverSide = LAST_CONN.get();
        waitFor(() -> budget.getGlobalUsedBytes() == 0, (WRITE_TIMEOUT_SEC + 8) * 1000L);
        waitFor(() -> !serverSide.isOpen(), 3_000);

        sock.setSoTimeout(5_000);
        InputStream in = sock.getInputStream();
        byte[] buf = new byte[64 * 1024];
        boolean reset = false;
        long received = 0;
        try
        {
            int got = in.read(buf);
            while (got >= 0)
            {
                received += got;
                got = in.read(buf);
            }
        }
        catch (java.net.SocketException exc)
        {
            reset = true;
        }
        assertTrue("expected a connection reset after " + received + " bytes, got EOF", reset);
    }
}
