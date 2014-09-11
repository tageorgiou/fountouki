package georgiou.thomas.fountouki;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.nifty.client.NiftyClient;
import com.facebook.nifty.client.NiftyClientChannel;
import com.facebook.nifty.client.RequestChannel;
import com.facebook.nifty.core.TChannelBufferInputTransport;
import com.facebook.nifty.core.TChannelBufferOutputTransport;
import com.facebook.nifty.duplex.TProtocolPair;
import com.facebook.nifty.duplex.TTransportPair;
import com.facebook.swift.service.ThriftClientConfig;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by tgeorgiou on 7/22/14.
 */
public class ReplayLog {
    public static void main(String[] args) {
        try {
            ReplayLog replayLog = new ReplayLog();
            replayLog.replayLog("requests.log");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void replayLog(String filename) throws ExecutionException, InterruptedException, TException {
        Path path = FileSystems.getDefault().getPath(filename);
        RequestReader requestReader = null;
        try {
            requestReader = new RequestReader(path);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection connection = new Connection(HostAndPort.fromParts("localhost", 8899));
        while (requestReader.hasNext()) {
            LogEntry logEntry = requestReader.readLogEntry();
            connection.callMethod(logEntry);
        }
    }

    class Connection {
        TChannelBufferOutputTransport outputTransport;
        TChannelBufferInputTransport inputTransport;
        TProtocol outputProtocol;
        TProtocol inputProtocol;
        NiftyClientChannel niftyClientChannel;
        int seqId = 1;

        public Connection(HostAndPort hostAndPort) throws ExecutionException, InterruptedException {
            FramedClientConnector connector = new FramedClientConnector(hostAndPort);
            NiftyClient niftyClient = new NiftyClient();
            ListenableFuture<? extends NiftyClientChannel> future = niftyClient.connectAsync(
                    connector,
                    new Duration(200, TimeUnit.MILLISECONDS), // connect timeout
                    new Duration(4000, TimeUnit.MILLISECONDS), // receive timeout
                    new Duration(400, TimeUnit.MILLISECONDS), // read timeout
                    new Duration(200, TimeUnit.MILLISECONDS), // write timeout
                    ThriftClientConfig.DEFAULT_MAX_FRAME_SIZE,
                    niftyClient.getDefaultSocksProxyAddress());
            niftyClientChannel = future.get();
            inputTransport = new TChannelBufferInputTransport();
            outputTransport = new TChannelBufferOutputTransport();
            TTransportPair transportPair = TTransportPair.fromSeparateTransports(inputTransport, outputTransport);
            TProtocolPair protocolPair = niftyClientChannel.getProtocolFactory().getProtocolPair(transportPair);
            inputProtocol = protocolPair.getInputProtocol();
            System.out.println(inputProtocol.getClass().toString());
            outputProtocol = protocolPair.getOutputProtocol();
            System.out.println(outputProtocol.getClass().toString());
        }

        public void callMethod(LogEntry logEntry) throws TException, InterruptedException {
            outputTransport.resetOutputBuffer();
            outputProtocol.writeMessageBegin(new TMessage(logEntry.methodName, TMessageType.CALL, seqId++));
            // System.out.println("Request length: " + logEntry.request.length + " bytes");
            TMemoryInputTransport in = new TMemoryInputTransport(logEntry.request);
            TCompactProtocol iprot = new TCompactProtocol(in);
            TProtocolCopier.copy(iprot, outputProtocol, TType.STRUCT);
            outputProtocol.writeMessageEnd();
            outputProtocol.getTransport().flush();

            ChannelBuffer requestBuffer = outputTransport.getOutputBuffer();
            niftyClientChannel.sendAsynchronousRequest(requestBuffer, false, new RequestChannel.Listener() {
                @Override
                public void onRequestSent() {
                    //System.out.println("Request sent");
                }

                @Override
                public void onResponseReceived(ChannelBuffer channelBuffer) {
                    //System.out.println("Response received");
                }

                @Override
                public void onChannelError(TException e) {
                    e.printStackTrace();
                    System.out.println("Channel Error");
                }
            });
            Thread.sleep(10);
        }
    }
}
