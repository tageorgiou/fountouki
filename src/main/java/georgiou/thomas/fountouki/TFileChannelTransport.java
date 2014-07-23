package georgiou.thomas.fountouki;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

/**
 * Created by tgeorgiou on 7/22/14.
 */
public class TFileChannelTransport extends TTransport {
    private final FileChannel fileChannel;

    public TFileChannelTransport(Path path) throws IOException {
        fileChannel = FileChannel.open(path, EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE));
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void open() throws TTransportException {

    }

    @Override
    public void close() {

    }

    @Override
    public int read(byte[] bytes, int i, int i2) throws TTransportException {
        return 0;
    }

    @Override
    public void write(byte[] bytes, int i, int i2) throws TTransportException {

    }
}
