package kafka.stream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestProcessor implements Processor<String, String> {

	private ProcessorContext context;
	private WindowStore<String, String> stateStore;
	private String storeName;

	private final static Logger LOGGER = LoggerFactory.getLogger(RequestProcessor.class);

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {

		this.context = context;
		this.stateStore = (WindowStore<String, String>) context.getStateStore("requestStateStore");
		LOGGER.info("CONTEXT: {}, STATE-STORE: {}, STATE-STORE-NAME: {}, STATE-STORE-CLASS: {}",this.context,this.stateStore,this.storeName,this.stateStore.getClass());

	}

	@Override
	public void process(String key, String value) {
		LOGGER.info("******************* REQUEST-PROCESS ******************** ");
		
		this.stateStore.put(key, value, System.currentTimeMillis());

		KeyValueIterator<Windowed<String>, String> storeIterator = this.stateStore.all();;
		while(storeIterator.hasNext()) {
			KeyValue<Windowed<String>, String> entry = storeIterator.next();
			LOGGER.info("DB KEY: {}",entry.key.key());
			LOGGER.info("WINDOW: {}",entry.key.window());
		}
		storeIterator.close();
	}

	@Override
	public void close() {}

}
