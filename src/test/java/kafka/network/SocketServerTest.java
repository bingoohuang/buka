package kafka.network;

import com.google.common.collect.Maps;
import kafka.api.ProducerRequest;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.producer.SyncProducerConfigs;
import kafka.utils.Function0;
import kafka.utils.TestUtils;
import org.junit.After;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Random;

import static org.junit.Assert.*;

public class SocketServerTest {
    SocketServer server = new SocketServer(0,
                                               /* host = */null,
                                               /* port = */kafka.utils.TestUtils.choosePort(),
                                              /*  numProcessorThreads =*/ 1,
                                              /*  maxQueuedRequests =*/ 50,
                                              /*  sendBufferSize =*/ 300000,
                                               /* recvBufferSize =*/ 300000,
                                               /* maxRequestSize =*/ 50);

    {
        server.startup();
    }

    public void sendRequest(Socket socket, short id, byte[] request) throws IOException {
        DataOutputStream outgoing = new DataOutputStream(socket.getOutputStream());
        outgoing.writeInt(request.length + 2);
        outgoing.writeShort(id);
        outgoing.write(request);
        outgoing.flush();
    }

    public byte[] receiveResponse(Socket socket) throws IOException {
        DataInputStream incoming = new DataInputStream(socket.getInputStream());
        int len = incoming.readInt();
        byte[] response = new byte[len];
        incoming.readFully(response);

        return response;
    }

    /* A simple request handler that just echos back the response */
    public void processRequest(RequestChannel channel) {
        Request request = channel.receiveRequest();
        ByteBuffer byteBuffer = ByteBuffer.allocate(request.requestObj.sizeInBytes());
        request.requestObj.writeTo(byteBuffer);
        byteBuffer.rewind();
        BoundedByteBufferSend send = new BoundedByteBufferSend(byteBuffer);
        channel.sendResponse(new Response(request.processor, request, send));
    }

    public Socket connect() throws IOException {
        return new Socket("localhost", server.port);
    }

    @After
    public void cleanup() {
        server.shutdown();
    }

    @Test
    public void simpleRequest() throws IOException {
        Socket socket = connect();
        int correlationId = -1;
        String clientId = SyncProducerConfigs.DefaultClientId;
        int ackTimeoutMs = SyncProducerConfigs.DefaultAckTimeoutMs;
        short ack = SyncProducerConfigs.DefaultRequiredAcks;
        ProducerRequest emptyRequest =
                new ProducerRequest(correlationId, clientId, ack, ackTimeoutMs, Maps.<TopicAndPartition, ByteBufferMessageSet>newHashMap());

        ByteBuffer byteBuffer = ByteBuffer.allocate(emptyRequest.sizeInBytes());
        emptyRequest.writeTo(byteBuffer);
        byteBuffer.rewind();
        byte[] serializedBytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(serializedBytes);

        sendRequest(socket, (short) 0, serializedBytes);
        processRequest(server.requestChannel);
        assertArrayEquals(serializedBytes, receiveResponse(socket));
    }

    @Test(expected = IOException.class)
    public void tooBigRequestIsRejected() throws IOException {
        byte[] tooManyBytes = new byte[server.maxRequestSize + 1];
        new Random().nextBytes(tooManyBytes);
        Socket socket = connect();
        sendRequest(socket, (short) 0, tooManyBytes);
        receiveResponse(socket);
    }

    @Test
    public void testSocketSelectionKeyState() throws IOException {
        Socket socket = connect();
        int correlationId = -1;
        String clientId = SyncProducerConfigs.DefaultClientId;
        int ackTimeoutMs = SyncProducerConfigs.DefaultAckTimeoutMs;
        short ack = 0;
        ProducerRequest emptyRequest =
                new ProducerRequest(correlationId, clientId, ack, ackTimeoutMs, Maps.<TopicAndPartition, ByteBufferMessageSet>newHashMap());

        ByteBuffer byteBuffer = ByteBuffer.allocate(emptyRequest.sizeInBytes());
        emptyRequest.writeTo(byteBuffer);
        byteBuffer.rewind();
        byte[] serializedBytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(serializedBytes);

        sendRequest(socket, (short) 0, serializedBytes);

        final Request request = server.requestChannel.receiveRequest();
        // Since the response is not sent yet, the selection key should not be readable.
        assertFalse((((SelectionKey) request.requestKey).interestOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ);

        server.requestChannel.sendResponse(new Response(0, request, null));

        // After the response is sent to the client (which is async and may take a bit of time), the socket key should be available for reads.
        assertTrue(
                TestUtils.waitUntilTrue(new Function0<Boolean>() {
                    @Override
                    public Boolean apply() {
                        return (((SelectionKey) request.requestKey).interestOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ;
                    }
                }, 5000));
    }
}
