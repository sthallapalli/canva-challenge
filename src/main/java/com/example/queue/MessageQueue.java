package com.example.queue;

import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.sqs.model.Message;

/**
 * As ScheduledExecutorService passed by the client, client is responsible for
 * shutting down the scheduler service.
 * 
 * @author <a href="mailto:sthallapalli@outlook.com">sthallapalli</a>
 * @since 21-Aug-2017
 */
public class MessageQueue<T extends BlockingDeque<Message>> {

	private static final Logger LOG = Logger.getLogger(MessageQueue.class.getName());

	private long visibilityTimeout = 2000;
	private ScheduledExecutorService executorService;
	private final ConcurrentMap<String, ScheduledFuture<?>> inFlightMessages = new ConcurrentHashMap<>();
	private final T queue;

	public MessageQueue(T queue, ScheduledExecutorService executorService) {
		Objects.requireNonNull(queue, "Queue can not be null.");
		Objects.requireNonNull(executorService, "Executor service can not be null.");

		this.visibilityTimeout = 2000;
		this.queue = queue;
		this.executorService = executorService;
	}

	public void push(Message message) {
		Objects.requireNonNull(message);
		this.queue.offerLast(message);
	}

	@SuppressWarnings("rawtypes")
	public Message poll() {
		Message message = this.queue.pollFirst();

		// Scheduling the message to add to head, in case of visibility timeout
		Runnable runnable = () -> {
			this.queue.offerFirst(message);
			this.inFlightMessages.remove(message.getMessageId());
		};

		if (message != null && message.getBody() != null) {
			ScheduledFuture future = executorService.schedule(runnable, this.visibilityTimeout, TimeUnit.MILLISECONDS);
			// Cache the polled message to simulate the suppression of the
			// message.
			this.inFlightMessages.put(message.getMessageId(), future);
		}
		return message;
	}

	public boolean delete(String receiptHandle) {
		if (receiptHandle == null || !this.inFlightMessages.containsKey(receiptHandle))
			return false;

		ScheduledFuture<?> future = this.inFlightMessages.remove(receiptHandle);
		if (future == null) {
			LOG.log(Level.SEVERE, "Message not found in inflight messages for receiptHandle [" + receiptHandle + "].");
			throw new RuntimeException(
					"Message not found in inflight messages for receiptHandle [" + receiptHandle + "].");
		}
		// Cancel the re-insertion of the message to queue head
		return future.cancel(true);
	}

	public void withVisibilityTimeout(long visibilityTimeout) {
		this.visibilityTimeout = visibilityTimeout;
	}

	public int inFlightSize() {
		return this.inFlightMessages.size();
	}

	public int size() {
		return this.queue.size();
	}
}
