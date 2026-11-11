package com.linbit.linstor.api.rest.v1.config;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.annotation.Priority;
import jakarta.inject.Provider;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.Request;

/**
 * Puts large REST answers under a {@link ResponseBudget} and writes them in chunks.
 *
 * Budget: a String or byte[] entity larger than {@link #LARGE_ENTITY_BYTES} takes a lease for its encoded size. If
 * the global or the client's budget is exhausted, the body is dropped and the answer becomes 503 with Retry-After.
 *
 * Chunked write: the body is written in {@link #CHUNK_BYTES} pieces with a flush after each. Grizzly's OutputBuffer
 * queues a whole write before it checks whether the connection can take more, so a single 14 MB write to a client
 * that does not read ends up entirely in the async write queue. Per chunk, the flush blocks once the queue is full,
 * and the transport write timeout ends the write of a stalled client after one chunk instead of after the body.
 *
 * The lease is returned when the write ends, also on IOException (client gone, write timeout). If the write never
 * runs (e.g. a later filter fails), ResponseBudget's 10-minute safety net returns the lease on a later acquire.
 *
 * Failed write: the connection is terminated, not closed. Grizzly's graceful close waits until the queued bytes are
 * sent, which for a client that does not read means until the client itself closes. SO_LINGER 0 is set before the
 * terminate, so the kernel drops the unread send buffer with a reset instead of a FIN behind it.
 *
 * Costs and limits:
 * - The write runs on the thread that resumed the answer (the RestResponse scheduler); a stalled client blocks
 *   that thread until the write timeout. That was already so before the budget; the budget caps how many large answers
 *   can be stalled at once.
 * - The budget counts the encoded bytes. While the filter encodes, the String and its bytes both exist; the String
 *   is unreferenced once the entity is replaced.
 * - Only entities over 1 MiB are budgeted. HEAD answers and non-2xx answers are left alone.
 *
 * Priority 1: response filters run in descending priority, so this one runs after the other response filters and
 * sees the final entity.
 */
@Priority(1)
public class ResponseBudgetFilter implements ContainerResponseFilter
{
    static final int LARGE_ENTITY_BYTES = 1024 * 1024;
    static final int CHUNK_BYTES = 256 * 1024;
    static final String RETRY_AFTER_SEC = "5";
    static final String UNKNOWN_CLIENT = "unknown";
    /** Upper bound of UTF-8 bytes per Java char (3 is the true maximum; 4 keeps the pre-check conservative). */
    private static final int MAX_UTF8_BYTES_PER_CHAR = 4;

    /**
     * Pending bytes per connection in Grizzly's async write queue before a write blocks. Grizzly's
     * default (AUTO = 4 x the socket's send buffer) can take a whole large answer, so the write would return at once
     * while the body sits in the queue outside the budget.
     */
    static final int MAX_PENDING_BYTES_PER_CONNECTION = 2 * CHUNK_BYTES;

    private final ResponseBudget budget;
    private final @Nullable ErrorReporter errorReporter;
    private final AtomicBoolean warnedUnknownClient = new AtomicBoolean(false);

    @Context
    private @Nullable Provider<Request> grizzlyRequestProvider;

    public ResponseBudgetFilter(ResponseBudget budgetRef, @Nullable ErrorReporter errorReporterRef)
    {
        budget = budgetRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void filter(ContainerRequestContext requestCtx, ContainerResponseContext responseCtx)
    {
        @Nullable Request grizzlyRequest = grizzlyRequest();
        // the render ticket of this request (from RequestHelper), with a reservation if it was predicted large
        ResponseBudget.@Nullable RenderTicket ticket = takeTicket(grizzlyRequest);

        Object entity = responseCtx.getEntity();
        @Nullable byte[] body = null;
        long size = -1;
        boolean budgeted = !"HEAD".equalsIgnoreCase(requestCtx.getMethod()) &&
            responseCtx.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL;
        if (!budgeted)
        {
            // HEAD: Jersey drops the body; non-2xx: error answers are small and never held for long
            entity = null;
        }
        if (entity instanceof String str)
        {
            // a String of fewer chars than the threshold can still encode to more bytes; check the encoded size
            if (str.length() > LARGE_ENTITY_BYTES / MAX_UTF8_BYTES_PER_CHAR)
            {
                body = str.getBytes(StandardCharsets.UTF_8);
                size = body.length;
            }
            else
            {
                size = str.length(); // a lower bound, enough to know the answer is small
            }
        }
        else if (entity instanceof byte[] bytes)
        {
            body = bytes;
            size = body.length;
        }

        if (size >= 0 && grizzlyRequest != null)
        {
            // remember every 2xx answer's size, small ones too, so a target whose answer shrank stops being gated
            budget.recordAnswerSize(clientKey(grizzlyRequest), requestTarget(grizzlyRequest), size);
        }
        if (body != null && body.length > LARGE_ENTITY_BYTES)
        {
            String client = clientKey(grizzlyRequest);
            ResponseBudget.@Nullable Lease lease = ticket != null ?
                ticket.toLease(body.length) :
                budget.tryAcquire(client, body.length);
            if (lease == null)
            {
                refuse(responseCtx, client, body.length);
            }
            else
            {
                // no explicit Content-Length: Grizzly may gzip the body (compression is on for JSON) and frames it
                MediaType mediaType = responseCtx.getMediaType();
                responseCtx.setEntity(new ChunkedBody(body, lease, connectionOf(grizzlyRequest)), null, mediaType);
            }
        }
        else if (ticket != null)
        {
            // predicted large, but small (or not budgeted) after all
            ticket.releaseReservation();
        }
    }

    private static ResponseBudget.@Nullable RenderTicket takeTicket(@Nullable Request req)
    {
        ResponseBudget.@Nullable RenderTicket ticket = null;
        if (req != null)
        {
            Object attr = req.getAttribute(ResponseBudget.RENDER_TICKET_ATTR);
            if (attr instanceof ResponseBudget.RenderTicket renderTicket)
            {
                req.removeAttribute(ResponseBudget.RENDER_TICKET_ATTR);
                ticket = renderTicket;
            }
        }
        return ticket;
    }

    /** Bound the async write queue of every listener of the server. Call before start(). */
    public static void limitPendingBytes(HttpServer server)
    {
        for (NetworkListener listener : server.getListeners())
        {
            listener.setMaxPendingBytes(MAX_PENDING_BYTES_PER_CONNECTION);
        }
    }

    private void refuse(ContainerResponseContext responseCtx, String client, long bytes)
    {
        if (errorReporter != null)
        {
            errorReporter.logWarning(
                "REST answer of %d bytes to %s refused: response budget exhausted (in use %d of %d bytes)",
                bytes,
                client,
                budget.getGlobalUsedBytes(),
                budget.getGlobalCapBytes()
            );
        }
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_UNKNOWN_ERROR,
                "Controller REST response budget exhausted, retry in " + RETRY_AFTER_SEC + " s"
            )
        );
        Response answer = ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.SERVICE_UNAVAILABLE);
        responseCtx.setStatus(Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
        responseCtx.getHeaders().putSingle("Retry-After", RETRY_AFTER_SEC);
        responseCtx.getHeaders().remove(HttpHeaders.CONTENT_LENGTH);
        responseCtx.setEntity(answer.getEntity(), null, MediaType.APPLICATION_JSON_TYPE);
    }

    private @Nullable Request grizzlyRequest()
    {
        @Nullable Request req = null;
        try
        {
            if (grizzlyRequestProvider != null)
            {
                req = grizzlyRequestProvider.get();
            }
        }
        catch (RuntimeException ignored)
        {
            // no request in scope
        }
        return req;
    }

    /** "path?query" of the request, the key for the size prediction. */
    public static String requestTarget(Request req)
    {
        String query = req.getQueryString();
        return query == null ? req.getRequestURI() : req.getRequestURI() + "?" + query;
    }

    private static @Nullable Connection<?> connectionOf(@Nullable Request req)
    {
        @Nullable Connection<?> conn = null;
        try
        {
            if (req != null && req.getContext() != null)
            {
                conn = req.getContext().getConnection();
            }
        }
        catch (RuntimeException ignored)
        {
            // cannot terminate on a failed write; the lease is still returned
        }
        return conn;
    }

    public static String clientKeyOf(@Nullable Request req)
    {
        String key = UNKNOWN_CLIENT;
        try
        {
            if (req != null && req.getRemoteAddr() != null)
            {
                key = req.getRemoteAddr();
            }
        }
        catch (RuntimeException ignored)
        {
            // no address: all such answers share one client budget
        }
        return key;
    }

    private String clientKey(@Nullable Request req)
    {
        String key = clientKeyOf(req);
        if (UNKNOWN_CLIENT.equals(key) && errorReporter != null && warnedUnknownClient.compareAndSet(false, true))
        {
            errorReporter.logWarning(
                "REST response budget: client address not available; such answers share one per-client budget"
            );
        }
        return key;
    }

    /** Writes the body in chunks and returns the lease when the write ends. */
    static final class ChunkedBody implements StreamingOutput
    {
        private final byte[] body;
        private final ResponseBudget.Lease lease;
        private final @Nullable Connection<?> connection;

        ChunkedBody(byte[] bodyRef, ResponseBudget.Lease leaseRef, @Nullable Connection<?> connectionRef)
        {
            body = bodyRef;
            lease = leaseRef;
            connection = connectionRef;
        }

        @Override
        public void write(OutputStream out) throws IOException
        {
            try
            {
                for (int off = 0; off < body.length; off += CHUNK_BYTES)
                {
                    out.write(body, off, Math.min(CHUNK_BYTES, body.length - off));
                    out.flush();
                }
            }
            catch (IOException exc)
            {
                // drop the connection and its queued bytes now (see class javadoc)
                if (connection != null)
                {
                    abortiveClose(connection);
                    connection.terminateWithReason(exc);
                }
                throw exc;
            }
            finally
            {
                lease.close();
            }
        }

        /**
         * SO_LINGER 0 before the terminate, so the close is a reset. Without it the kernel keeps the unread send
         * buffer (~200 KB per socket on the rack) in FIN-WAIT-1 until the client closes. The write has already failed
         * or timed out, so nothing in that buffer would be read.
         */
        static void abortiveClose(Connection<?> conn)
        {
            try
            {
                if (conn instanceof org.glassfish.grizzly.nio.NIOConnection nioConn &&
                    nioConn.getChannel() instanceof java.nio.channels.SocketChannel channel)
                {
                    channel.setOption(java.net.StandardSocketOptions.SO_LINGER, 0);
                }
            }
            catch (IOException | RuntimeException ignored)
            {
                // the terminate still closes the connection, gracefully
            }
        }

        ResponseBudget.Lease getLease()
        {
            return lease;
        }
    }
}
