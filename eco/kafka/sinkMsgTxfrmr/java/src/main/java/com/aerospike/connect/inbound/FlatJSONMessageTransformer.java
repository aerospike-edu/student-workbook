import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.Bin;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.connect.inbound.InboundMessageTransformer;
import com.aerospike.connect.inbound.model.InboundMessage;
import com.aerospike.connect.inbound.operation.AerospikeOperateOperation;
import com.aerospike.connect.inbound.operation.AerospikeRecordOperation;
import com.aerospike.connect.inbound.operation.AerospikeSkipRecordOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class FlatJSONMessageTransformer implements
        InboundMessageTransformer<InboundMessage<Object, Object>> {

    private static final Logger logger = LoggerFactory.getLogger(FlatJSONMessageTransformer.class.getName());

    @Override
    public AerospikeRecordOperation transform(InboundMessage<Object, Object> input) {
        Map<String, Object> fields = input.getFields();

        System.out.println("fields imported: "+fields);  //Helps to code to schema
        if(fields.isEmpty()) {
           System.out.println("fields is empty.");
           return new AerospikeSkipRecordOperation();
        }
        // Get the Aerospike key.
        Map<String, Object> metaField = (Map<String, Object>) fields.get("metadata");
        if(metaField == null) {   
           System.out.println("metaField is null.");
           return new AerospikeSkipRecordOperation();
        }
        if(metaField.isEmpty()) {
           System.out.println("metaField is empty.");
           return new AerospikeSkipRecordOperation();
        }
        System.out.println("metaField: "+ metaField);

        String ns = (String) metaField.get("namespace");
        String set = (String) metaField.get("set");
        String key = (String) metaField.get("userKey");

        if (key == null) {
            logger.warn("Invalid missing key");
            return new AerospikeSkipRecordOperation();
        }

        // Aerospike key.
        Key aerospikeKey = new Key(ns, set, key);

        // Bins
        // Get the bins
        @SuppressWarnings("unchecked")
        String vName = (String) fields.get("First_Name");
        long vAge = (long) fields.get("age");
        long vLut = (long) metaField.get("lut");

        Bin nameBin = new Bin("name", vName);
        Bin ageBin = new Bin("age", vAge);
        Bin lutBin = new Bin("LUT", vLut);

        // List to hold Aerospike operations.
        List<Operation> operations = new ArrayList<>();

        operations.add(Operation.put(nameBin));
        operations.add(Operation.put(ageBin));
        operations.add(Operation.put(lutBin));

        WritePolicy wPolicy = new WritePolicy();
        wPolicy.sendKey = true;

        return new AerospikeOperateOperation(aerospikeKey, wPolicy, operations, input.getIgnoreErrorCodes());
    }
}
