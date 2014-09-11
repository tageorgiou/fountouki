package georgiou.thomas.fountouki;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by tgeorgiou on 7/21/14.
 */
public class LogEntry {
    public final long timestamp;
    public final String methodName;
    public final byte[] request;

    public LogEntry(long timestamp, String methodName, TProtocol in) throws TException {
        this.timestamp = timestamp;
        this.methodName = methodName;

        TMemoryBuffer tMemoryBuffer = new TMemoryBuffer(1024);
        TCompactProtocol memProtocol = new TCompactProtocol(tMemoryBuffer);
        TProtocolCopier.copy(in, memProtocol, TType.STRUCT);
        tMemoryBuffer.flush();
        byte[] buf = new byte[tMemoryBuffer.length()];
        tMemoryBuffer.readAll(buf, 0, tMemoryBuffer.length());
        request = buf;
    }

    public LogEntry(long timestamp, String methodName, ByteBuffer request) throws TException {
        this.timestamp = timestamp;
        this.methodName = methodName;
        ByteBuffer in = request.asReadOnlyBuffer();
        in.rewind();
        byte[] bytes = new byte[in.remaining()];
        in.get(bytes);
        this.request = bytes;
    }

    public ByteBuffer getByteBuffer() {
       ByteBuffer byteBuffer = ByteBuffer.allocate(request.length + methodName.length() + 100);
        byteBuffer.putLong(timestamp);
        byteBuffer.putInt(methodName.length());
        byteBuffer.put(methodName.getBytes());
        byteBuffer.put(request);
        byteBuffer.flip();
        return byteBuffer;
    }

    public void write(TProtocol prot) throws TException {
        prot.writeStructBegin(new TStruct());
        prot.writeI64(timestamp);
        prot.writeString(methodName);
        prot.writeBinary(ByteBuffer.wrap(request));
        prot.writeStructEnd();
    }

    public static LogEntry readLogEntry(TProtocol prot) throws TException {
        prot.readStructBegin();
        long timestamp = prot.readI64();
        String methodName = prot.readString();
        ByteBuffer request = prot.readBinary();
        prot.readStructEnd();
        return new LogEntry(timestamp, methodName, request);
    }
}
