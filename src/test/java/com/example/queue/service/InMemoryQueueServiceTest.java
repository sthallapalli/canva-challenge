package com.example.queue.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.sqs.model.Message;
import com.example.queue.MessageQueue;
import com.example.queue.service.QueueService;
import com.example.queue.service.impl.InMemoryQueueService;
import com.example.queue.service.impl.MessageQueueServiceHelper;

/**
 * @author <a href="mailto:sthallapalli@outlook.com">sthallapalli</a>
 * @since 23-Aug-2017
 */

public class InMemoryQueueServiceTest {

	private InMemoryQueueService queueService;

	private ExecutorService executorService;

	@Mock
	private MessageQueueServiceHelper<LinkedBlockingDeque<Message>> queueServiceHelper;

	@Before
	public void init() {
		MockitoAnnotations.initMocks(this);

		executorService = Executors.newFixedThreadPool(10);
		ScheduledExecutorService mockScheduledService = mock(ScheduledExecutorService.class);
		ConcurrentMap<String, MessageQueue<LinkedBlockingDeque<Message>>> queues = mock(ConcurrentHashMap.class);
		MessageQueue<LinkedBlockingDeque<Message>> messageQueue = new MessageQueue<>(new LinkedBlockingDeque<>(),
				mockScheduledService);
		messageQueue.withVisibilityTimeout(200);
		queues.put(anyString(), eq(messageQueue));

		this.queueService = new InMemoryQueueService(queues);
		this.queueService.setMessageQueueHelper(queueServiceHelper);
	}

	@Test
	public void shouldHandleAddToQueueSimultaneously() throws InterruptedException {
		int executionTimes = 1000;
		when(this.queueServiceHelper.getMessageCount(anyString())).thenReturn(executionTimes);
		// Attempt adding a single element in the queue simultaneously
		final CountDownLatch latch = new CountDownLatch(executionTimes);
		List<Runnable> runnables = new ArrayList<>();
		for (int i = 0; i < executionTimes; i++) {
			runnables.add(() -> {
				try {
					this.queueService.sendMessage(anyString(), anyString());
				} finally {
					latch.countDown();
				}
			});
		}

		runnables.forEach((runnable) -> executorService.submit(runnable));
		latch.await();

		verify(this.queueServiceHelper, times(executionTimes)).sendMessage(anyString(), anyString());
		assertEquals(executionTimes, this.queueServiceHelper.getMessageCount("queue1"));
		executorService.shutdown();
	}

	@Test
	public void shouldHandlePollOnQueueSimultaneously() throws InterruptedException {
		int executionTimes = 1000;
		List<String> sentMessages = Collections.synchronizedList(new ArrayList<>());
		String message = "test message body";
		for (int i = 0; i < executionTimes; i++) {
			sentMessages.add(message);
			this.queueService.sendMessage(anyString(), anyString());
		}
		Message msg = new Message();
		msg.setBody(message);
		when(this.queueService.recieveMessage(anyString())).thenReturn(msg);

		final CountDownLatch latch = new CountDownLatch(executionTimes);
		List<Runnable> runnables = new ArrayList<>();
		for (int i = 0; i < executionTimes; i++) {
			runnables.add(() -> {
				try {
					String messageBody = queueService.recieveMessage(anyString()).getBody();
					assertTrue(sentMessages.remove(messageBody));
				} finally {
					latch.countDown();
				}
			});
		}
		runnables.forEach((runnable) -> executorService.submit(runnable));
		latch.await();

		verify(this.queueServiceHelper, times(executionTimes)).recieveMessage(anyString());
		// assertEquals(0, this.queueServiceHelper.getMessageCount("queue1"));
		assertEquals(0, sentMessages.size());
	}

	@Test
	public void shouldHandledelete() throws InterruptedException {
		int executionTimes = 1000;
		List<String> sentMessages = Collections.synchronizedList(new ArrayList<>());
		String message = "test message body";
		for (int i = 0; i < executionTimes; i++) {
			sentMessages.add(message);
			this.queueService.sendMessage(anyString(), anyString());
		}
		Message msg = new Message();
		msg.setBody(message);
		when(this.queueService.recieveMessage("queue1")).thenReturn(msg);

		final CountDownLatch latch = new CountDownLatch(executionTimes);
		List<Runnable> runnables = new ArrayList<>();
		for (int i = 0; i < executionTimes; i++) {
			runnables.add(() -> {
				try {
					Message msag = queueService.recieveMessage("queue1");
					this.queueService.deleteMessage("queue1", msag.getReceiptHandle());
					assertTrue(sentMessages.remove(msag.getBody()));
				} finally {
					latch.countDown();
				}
			});
		}
		runnables.forEach((runnable) -> executorService.submit(runnable));
		latch.await();

		verify(this.queueServiceHelper, times(executionTimes)).recieveMessage(anyString());
		assertEquals(0, sentMessages.size());
	}

	@Test
	public void testVisibilityTimeout() throws InterruptedException {

		ConcurrentMap<String, MessageQueue<LinkedBlockingDeque<Message>>> queues = new ConcurrentHashMap<>();
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

		MessageQueue<LinkedBlockingDeque<Message>> messageQueue = new MessageQueue<>(new LinkedBlockingDeque<>(),
				scheduler);
		messageQueue.withVisibilityTimeout(100);
		queues.put("queue1", messageQueue);

		QueueService service = new InMemoryQueueService(queues);
		service.sendMessage("queue1", "This is the message");
		Message msg = service.recieveMessage("queue1");

		// No wait for visitbility timeout, we expect null message here.
		msg = service.recieveMessage("queue1");
		Assert.assertNull(msg);

		// Waiting for visibility timeout, we expect the head message here.
		Thread.sleep(150);
		msg = service.recieveMessage("queue1");
		Assert.assertNotNull(msg);
		scheduler.shutdown();
	}
}
