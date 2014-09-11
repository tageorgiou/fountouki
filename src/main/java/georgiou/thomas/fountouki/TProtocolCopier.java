package georgiou.thomas.fountouki;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;

import java.nio.ByteBuffer;

/**
 * Created by tgeorgiou on 7/21/14.
 */
public class TProtocolCopier {
    public static void copy(TProtocol in, TProtocol out, byte type) throws TException {
        switch (type) {
            case TType.BOOL:
                out.writeBool(in.readBool());
                break;

            case TType.BYTE:
                out.writeByte(in.readByte());
                break;

            case TType.I16:
                out.writeI16(in.readI16());
                break;

            case TType.I32:
                out.writeI32(in.readI32());
                break;

            case TType.I64:
                out.writeI64(in.readI64());
                break;

            case TType.DOUBLE:
                out.writeDouble(in.readDouble());
                break;

            case TType.STRING:
                out.writeBinary(in.readBinary());
                break;

            case TType.STRUCT:
                out.writeStructBegin(in.readStructBegin());
                while (true) {
                    TField field = in.readFieldBegin();
                    if (field.type == TType.STOP) {
                        in.readI16(); // HACK to make old files work
                        out.writeFieldStop();
                        break;
                    }
                    out.writeFieldBegin(field);
                    copy(in, out, field.type);
                    in.readFieldEnd();
                    out.writeFieldEnd();
                }
                in.readStructEnd();
                out.writeStructEnd();
                break;

            case TType.MAP:
                TMap map = in.readMapBegin();
                out.writeMapBegin(map);
                for (int i = 0; i < map.size; i++) {
                    copy(in, out, map.keyType);
                    copy(in, out, map.valueType);
                }
                in.readMapEnd();
                out.writeMapEnd();
                break;

            case TType.SET:
                TSet set = in.readSetBegin();
                out.writeSetBegin(set);
                for (int i = 0; i < set.size; i++) {
                    copy(in, out, set.elemType);
                }
                in.readSetEnd();
                out.writeSetEnd();
                break;

            case TType.LIST:
                TList list = in.readListBegin();
                out.writeListBegin(list);
                for (int i = 0; i < list.size; i++) {
                    copy(in, out, list.elemType);
                }
                in.readListEnd();
                out.writeListEnd();
                break;

            default:
                break;
        }
    }
}
