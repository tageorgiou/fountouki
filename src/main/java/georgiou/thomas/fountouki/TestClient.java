package georgiou.thomas.fountouki;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.swift.service.ThriftClientManager;
import com.google.common.net.HostAndPort;

/**
 * Created by tgeorgiou on 7/21/14.
 */
public class TestClient {
    public static void main(String[] args) {
        try {
            runClient();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runClient() throws Exception {
        ThriftClientManager thriftClientManager = new ThriftClientManager();
        FramedClientConnector connector = new FramedClientConnector(HostAndPort.fromParts("localhost", 8899));
        ExampleService exampleService = thriftClientManager.createClient(connector, ExampleService.class).get();
        exampleService.doSomething();
    }
}
