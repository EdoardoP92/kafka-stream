package kafka.stream;

import javax.annotation.PostConstruct;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class App {

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}
	
	@PostConstruct
	public static void init() {
		
		String bootstrapServer = "localhost:9091";		
		String leftTopic = "parserTopicChallenge";
		String rightTopic = "parserTopicEsitoSCA";
		String joinTopic = "reconcilerTopic";
		
		new StreamK().processorApiJoinTopology(leftTopic, rightTopic, joinTopic, bootstrapServer)
			.start();
		
	}

}
