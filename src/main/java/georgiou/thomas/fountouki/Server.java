package georgiou.thomas.fountouki;

import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServerConfig;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.file.FileSystems;

public class Server
{
    public static void main( String[] args )
    {
        startServer(8899);
    }

    static void startServer(int port) {
        RequestWriter requestWriter = null;
        try {
            requestWriter = new RequestWriter(FileSystems.getDefault().getPath("requests.log"));
        } catch (IOException|TTransportException e) {
            e.printStackTrace();
            System.exit(1);
        }
        FountoukiServiceProcessor processor = new FountoukiServiceProcessor(requestWriter);
        ThriftServer server = new ThriftServer(processor, new ThriftServerConfig().setPort(port));
        server.start();
        System.out.println("Running server");
    }
}
