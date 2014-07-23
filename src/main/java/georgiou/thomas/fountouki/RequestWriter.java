package georgiou.thomas.fountouki;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFileTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

/**
 * Created by tgeorgiou on 7/21/14.
 */
public class RequestWriter {
    private BufferedOutputStream bufferedOutputStream;
    private TTransport trans;
    private TProtocol prot;

    public RequestWriter(Path path) throws IOException, TTransportException {
        String sPath = path.toFile().getPath();
        bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(sPath));
        trans = new TIOStreamTransport(bufferedOutputStream);
        prot = new TCompactProtocol(trans);
    }

    public synchronized void write(LogEntry logEntry) throws TException {
        logEntry.write(prot);
        trans.flush();
        try {
            bufferedOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void close() {
        trans.close();
    }
}
