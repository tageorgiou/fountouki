package georgiou.thomas.fountouki;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFileTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * Created by tgeorgiou on 7/22/14.
 */
public class RequestReader {
    private BufferedInputStream bufferedInputStream;
    private TTransport itrans;
    private TProtocol iprot;

    public RequestReader(Path path) throws IOException {
        String sPath = path.toFile().getPath();
        bufferedInputStream = new BufferedInputStream(new FileInputStream(sPath));
        itrans = new TIOStreamTransport(bufferedInputStream);
        iprot = new TCompactProtocol(itrans);
    }

    public synchronized LogEntry readLogEntry() throws TException {
        return LogEntry.readLogEntry(iprot);
    }

    public boolean hasNext() {
        return true;
    }
}
