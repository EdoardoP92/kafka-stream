package kafka.stream;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseProcessor implements Processor<String, String> {

	private ProcessorContext context;
	private WindowStore<String, String> stateStore;
	private String storeName;
	private KeyValueIterator<Windowed<String>, String> storeIterator;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ResponseProcessor.class);
	
	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {

		this.context = context;
		this.stateStore = (WindowStore<String, String>) context.getStateStore("requestStateStore");
		LOGGER.info("CONTEXT: {}, STATE-STORE: {}, STATE-STORE-NAME: {}",this.context,this.stateStore,this.storeName);
		
	}

	@Override
	public void process(String key, String value) {
		
		LOGGER.info("******************* RESPONSE-PROCESS ******************** ");
		
//		this.storeIterator = this.stateStore.all();
//		
//		while(storeIterator.hasNext()) {
//			KeyValue<Windowed<String>, String> entry = storeIterator.next();
//			if(entry.key.key().equals(key) && entry.key.window().end() >= System.currentTimeMillis()) {
//				this.context.forward(key, "Hello ProcessorApi "+entry.value+" "+value);
//				this.context.commit();
//				LOGGER.info("COUPLED VALUE 1: {}, VALUE 2: {}",entry.value, value);
//				break;
//			}
//		}
//		this.storeIterator.close();
		
		String val = this.stateStore.fetch(key, System.currentTimeMillis()-Duration.ofMinutes(2).toMillis());
		System.out.println("+++++++++++++++++++ "+val);
		if(val != null) {
			this.context.forward(key, "Hello ProcessorApi "+val+" "+value);
			this.context.commit();
			LOGGER.info("COUPLED VALUE 1: {}, VALUE 2: {}",val, value);
		}else {
			LOGGER.info("NO VALUE FOUND FOR KEY: {}",key);
		}
	}

	@Override
	public void close() {}

}
