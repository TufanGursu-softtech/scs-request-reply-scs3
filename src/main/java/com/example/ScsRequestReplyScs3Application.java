package com.example;

import static java.lang.System.currentTimeMillis;

import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.dsl.HeaderEnricherSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.json.ObjectToJsonTransformer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("deprecation")
@EnableBinding({ Processor.class, ScsRequestReplyScs3Application.GatewayChannels.class })
@SpringBootApplication
public class ScsRequestReplyScs3Application {

	public static void main(String[] args)
			throws InterruptedException, StreamReadException, DatabindException, IOException {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(ScsRequestReplyScs3Application.class,
				args);
		QueueGateway gateway = applicationContext.getBean(QueueGateway.class);
		ObjectMapper objectMapper = new ObjectMapper();
		Thread.sleep(5000);

		while (true) {
			var ping = Ping.builder().cid("123456").currentTimeMillis(currentTimeMillis()).build();
			var byteArray = gateway.handle(ping);
			var pong = objectMapper.readValue(byteArray, Pong.class);

			System.out.println(pong);
			Thread.sleep(1000);
		}
	}

	interface GatewayChannels {
		String REQUEST = "request";
		String REPLY = "reply";

		@Output(REQUEST)
		MessageChannel requestChannel();

		@Input(REPLY)
		SubscribableChannel replyChannel();
	}

	public static final String HANDLER_FLOW = "handlerFlow";

	@Bean
	public IntegrationFlow requestsFlow() {
		return IntegrationFlows.from(HANDLER_FLOW).enrichHeaders(HeaderEnricherSpec::headerChannelsToString)
				.transform(new ObjectToJsonTransformer()).channel(GatewayChannels.REQUEST).get();
	}

	@MessagingGateway
	public interface QueueGateway {
		@Gateway(requestChannel = HANDLER_FLOW, replyChannel = GatewayChannels.REPLY)
		byte[] handle(@Payload Ping payload);
	}

	@StreamListener(Processor.INPUT)
	@SendTo(Processor.OUTPUT)
	public Pong handleMessage(Ping message, MessageHeaders msgHeaders) {
		System.out.println("Consumer %s".formatted(message));
		return Pong.builder().responseCid(message.getCid())
				.duration(currentTimeMillis() - message.getCurrentTimeMillis()).build();
	}

}
