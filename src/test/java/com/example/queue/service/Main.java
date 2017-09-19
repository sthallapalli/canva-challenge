package com.example.queue.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;

import com.amazonaws.services.sqs.model.Message;
import com.example.queue.FileQueue;
import com.example.queue.MessageQueue;
import com.example.queue.service.QueueService;
import com.example.queue.service.impl.FileSystemQueueService;
import com.example.queue.service.impl.InMemoryQueueService;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		//testInMemory();
		testFileSystem();
	}

	private static void testFileSystem() throws InterruptedException {
		
		ConcurrentMap<String, MessageQueue<FileQueue<Message>>> queues = new ConcurrentHashMap<>();
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
		MessageQueue<FileQueue<Message>> messageQueue = new MessageQueue<>(new FileQueue<>("queue1"), scheduler);
		messageQueue.withVisibilityTimeout(300);
		queues.put("queue1", messageQueue);
		QueueService service = new FileSystemQueueService(queues);
	
		service.sendMessage("queue1", "This is the message");
		Message msg = service.recieveMessage("queue1");
		System.out.println("From client : " + msg.toString());
		
		//boolean deleted = service.deleteMessage("queue1", msg.getReceiptHandle());
		//System.out.println("Deleted ? " + deleted);
		
		//Thread.sleep(200);
		Message msg1 = service.recieveMessage("queue1");
		System.out.println("From client : " + msg1);
		
		Thread.sleep(350);
		
		Message msg2 = service.recieveMessage("queue1");
		System.out.println("From client : " + msg2);
		
		scheduler.shutdown();
	}
	
	private static void testInMemory() throws InterruptedException {

		ConcurrentMap<String, MessageQueue<LinkedBlockingDeque<Message>>> queues = new ConcurrentHashMap<>();
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
		MessageQueue<LinkedBlockingDeque<Message>> messageQueue = new MessageQueue<>(new LinkedBlockingDeque<>(), scheduler);
		messageQueue.withVisibilityTimeout(100);
		queues.put("queue1", messageQueue);
		QueueService service = new InMemoryQueueService(queues);

/*		Runnable beforeVisibilityTimeout = () -> {
			Message m = service.recieveMessage("queue1");
			Assert.assertNull(m);
			System.out.println("Reached here. beforeVisibilityTimeout" + m.getBody());
		};
		
		Runnable afterVisibilityTimeout = () -> {
			Message m = service.recieveMessage("queue1");
			System.out.println("afterVisibilityTimeout " + m);
			Assert.assertNotNull(m);
			Assert.assertEquals(m.getBody(), "This is the message");
			System.out.println("Reached here. afterVisibilityTimeout");
		};
		
		ScheduledExecutorService visibilityTimeoutScheduler1 = Executors.newSingleThreadScheduledExecutor();
		ScheduledExecutorService visibilityTimeoutScheduler2 = Executors.newSingleThreadScheduledExecutor();


		service.sendMessage("queue1", "This is the message");
		visibilityTimeoutScheduler1.schedule(beforeVisibilityTimeout, 500, TimeUnit.MILLISECONDS);
		visibilityTimeoutScheduler2.schedule(afterVisibilityTimeout, 1100, TimeUnit.MILLISECONDS);*/
		service.sendMessage("queue1", "This is the message");
		Message msg = service.recieveMessage("queue1");
		System.out.println(msg.toString());
		
		// No wait for visitbility timeout, we expect null message here.
		msg = service.recieveMessage("queue1");
		System.out.println(msg);
		
		// Waiting for visibility timeout, we expect the head message here.
		Thread.sleep(150);
		msg = service.recieveMessage("queue1");
		System.out.println(msg);
		
		
		
		if (msg != null)
			System.out.println(String.valueOf(service.deleteMessage("queue1", msg.getReceiptHandle())));
		//visibilityTimeoutScheduler1.shutdown();
		//visibilityTimeoutScheduler2.shutdown();
		scheduler.shutdown();
	}
}
