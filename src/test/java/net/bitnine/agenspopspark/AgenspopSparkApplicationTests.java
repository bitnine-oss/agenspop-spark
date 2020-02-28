package net.bitnine.agenspopspark;

import net.bitnine.agenspopspark.job.Greeting;
import net.bitnine.agenspopspark.dto.HelloMessage;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// **NOTE reference
// https://github.com/spring-guides/gs-messaging-stomp-websocket/blob/master/complete/src/test/java/hello/GreetingIntegrationTests.java

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class AgenspopSparkApplicationTests {

	@LocalServerPort
	private int port;

	private SockJsClient sockJsClient;
	private WebSocketStompClient stompClient;
	private final WebSocketHttpHeaders headers = new WebSocketHttpHeaders();

	@Before
	public void setup() {
		List<Transport> transports = new ArrayList<>();
		transports.add(new WebSocketTransport(new StandardWebSocketClient()));
		this.sockJsClient = new SockJsClient(transports);

		this.stompClient = new WebSocketStompClient(sockJsClient);
		this.stompClient.setMessageConverter(new MappingJackson2MessageConverter());
	}

	@Test
	public void getGreeting() throws Exception {

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Throwable> failure = new AtomicReference<>();

		StompSessionHandler handler = new TestSessionHandler(failure) {

			@Override
			public void afterConnected(final StompSession session, StompHeaders connectedHeaders) {
				// receive Message
				session.subscribe("/topic/greetings", new StompFrameHandler() {
					@Override
					public Type getPayloadType(StompHeaders headers) {
						return Greeting.class;
					}

					@Override
					public void handleFrame(StompHeaders headers, Object payload) {
						Greeting greeting = (Greeting) payload;
						System.out.println("[2] receive from '/topic/greetings' => "+greeting.toString());
						try {
							assertEquals("Hello, Spring!", greeting.getContent());
						} catch (Throwable t) {
							failure.set(t);
						} finally {
							session.disconnect();
							latch.countDown();
						}
					}
				});
				// send Message
				try {
					System.out.println("[1] send to 'Spring' => /app/hello");
					session.send("/app/hello", new HelloMessage("Spring"));
				} catch (Throwable t) {
					failure.set(t);
					latch.countDown();
				}
			}
		};

		System.out.println("[0] connect => "+String.format("ws://localhost:%d/agenspop", this.port));
		this.stompClient.connect("ws://localhost:{port}/agenspop", this.headers, handler, this.port);

		if (latch.await(3, TimeUnit.SECONDS)) {
			System.out.println("[3] ws test done.\n");
			if (failure.get() != null) {
				throw new AssertionError("", failure.get());
			}
		}
		else {
			fail("Greeting not received");
		}

	}

	private class TestSessionHandler extends StompSessionHandlerAdapter {

		private final AtomicReference<Throwable> failure;

		public TestSessionHandler(AtomicReference<Throwable> failure) {
			this.failure = failure;
		}

		@Override
		public void handleFrame(StompHeaders headers, Object payload) {
			this.failure.set(new Exception(headers.toString()));
		}

		@Override
		public void handleException(StompSession s, StompCommand c, StompHeaders h, byte[] p, Throwable ex) {
			this.failure.set(ex);
		}

		@Override
		public void handleTransportError(StompSession session, Throwable ex) {
			this.failure.set(ex);
		}
	}

}
