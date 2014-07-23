package georgiou.thomas.fountouki;

import com.facebook.nifty.core.RequestContext;
import com.facebook.nifty.processor.NiftyProcessor;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TMemoryBuffer;

import java.io.IOException;

/**
 * Created by tgeorgiou on 7/21/14.
 */
public class FountoukiServiceProcessor implements NiftyProcessor {
    private final RequestWriter requestWriter;

    public FountoukiServiceProcessor(RequestWriter requestWriter) {
        this.requestWriter = requestWriter;
    }

    @Override
    public ListenableFuture<Boolean> process(TProtocol in, TProtocol out, RequestContext requestContext) throws TException {
        try {
            TMessage message = in.readMessageBegin();
            String methodName = message.name;
            int sequenceId = message.seqid;
            System.out.println("Got method " + methodName);

            LogEntry logEntry = new LogEntry(System.currentTimeMillis(), methodName, in);
            in.readMessageEnd();

            requestWriter.write(logEntry);

            out.writeMessageBegin(new TMessage(methodName, TMessageType.REPLY, sequenceId));
            out.writeStructBegin(new TStruct());

            // response field
            out.writeFieldBegin(new TField("success", (byte) 0, (short) 1));
            out.writeStructBegin(new TStruct());
            out.writeStructEnd();
            out.writeFieldEnd();

            out.writeStructEnd();
            out.writeMessageEnd();
            out.getTransport().flush();
        } catch (TException e) {
            e.printStackTrace();
            throw new TException(e);
        }

        return Futures.immediateFuture(true);
    }
}
