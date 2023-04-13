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
public class JSONMessageTransformer implements
        InboundMessageTransformer<InboundMessage<Object, Object>> {

    private static final Logger logger = LoggerFactory.getLogger(JSONMessageTransformer.class.getName());

    @Override
    public AerospikeRecordOperation transform(InboundMessage<Object, Object> input) {
        Map<String, Object> fields = input.getFields();

        System.out.println("fields imported: "+fields);  //Helps to code to schema
        if(fields.isEmpty()) {
           System.out.println("fields is empty.");
           return new AerospikeSkipRecordOperation();
        }
        /*
        fields imported: 
        {msg=write, gen=85, 
         bins=[{name=First_Name, type=str, value=Piyush}, {name=age, type=int, value=84}], 
         lut=1681341978142, exp=1681773978, 
         key=[test, testset, eY2wj4aI00AbI/982RCeFBQ+2jk=, key10]}
        */
        // Get the Aerospike key.
        List<String> keyList = new ArrayList<>();
        keyList.addAll( (List<String>) fields.get("key"));
        if(keyList == null) {   
           System.out.println("keyList is null.");
           return new AerospikeSkipRecordOperation();
        }
        if(keyList.isEmpty()) {
           System.out.println("keyList is empty.");
           return new AerospikeSkipRecordOperation();
        }
        System.out.println("keyList: "+ keyList);

        String ns = keyList.get(0);
        String set = keyList.get(1);
        String key = keyList.get(3);  //2 is digest

        if (key == null) {
            logger.warn("Invalid missing key");
            return new AerospikeSkipRecordOperation();
        }

        // Aerospike key.
        Key aerospikeKey = new Key(ns, set, key);

        // Bins
        // Get the bins
        @SuppressWarnings("unchecked")
        List<Object> binObjects = new ArrayList<>();
        binObjects.addAll((List<Object>) fields.get("bins"));

        Map<String, Object> binItem;
        binItem = (Map<String, Object>) binObjects.get(0);
        String vName = (String) binItem.get("value");
        
        binItem = (Map<String, Object>) binObjects.get(1);
        long vAge = (long) binItem.get("value");
        long vLut = (long) fields.get("lut");

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
