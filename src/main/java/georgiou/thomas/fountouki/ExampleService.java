package georgiou.thomas.fountouki;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

/**
 * Created by tgeorgiou on 7/21/14.
 */
@ThriftService
public interface ExampleService {
    @ThriftMethod
    void doSomething();
}