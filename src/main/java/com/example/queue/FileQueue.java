package com.example.queue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.sqs.model.Message;

/**
 * @author <a href="mailto:sthallapalli@outlook.com">sthallapalli</a>
 * @since 22-Aug-2017
 */
public class FileQueue<T> implements BlockingDeque<T> {

	private static final Logger LOG = Logger.getLogger(FileQueue.class.getName());

	private final String queueFolderName;
	private File messagesFile;

	public FileQueue(String queueFolderName) {
		this.queueFolderName = "/var/queues/" + queueFolderName;
		init(this.queueFolderName);
	}

	private void init(String queueFolderName) {
		File queueRoot = new File(queueFolderName);
		if (!queueRoot.exists())
			queueRoot.mkdirs();

		this.messagesFile = new File(queueRoot.getPath() + "/messages");

		if (!this.messagesFile.exists()) {
			try {
				this.messagesFile.createNewFile();
			} catch (IOException e) {
				LOG.log(Level.SEVERE, "Failed to create file [" + this.messagesFile.getPath() + "].");
				throw new RuntimeException("Failed to create file [" + this.messagesFile.getPath() + "].", e);
			}
		}
	}

	@Override
	public boolean offerFirst(T message) {
		File lockFile = getLockFile(queueFolderName);
		lock(lockFile);
		try {
			File tempFile = createTempFile();
			writeMessage(message);
			copyFile(messagesFile, tempFile, false);
			String messagesFilePath = messagesFile.getPath();
			messagesFile.delete();
			tempFile.renameTo(new File(messagesFilePath));
		} finally {
			unlock(lockFile);
		}
		return true;
	}

	@Override
	public boolean offerLast(T message) {
		File lockFile = getLockFile(queueFolderName);
		lock(lockFile);
		try {
			writeMessage(message);
		} finally {
			unlock(lockFile);
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T pollFirst() {
		File lockFile = getLockFile(this.queueFolderName);
		lock(lockFile);
		T message = null;
		try {
			String record = readFirstLine(this.messagesFile).orElse("");
			message = (T) ((record != null && !record.isEmpty()) ? getMessage(record) : new Message());
			if (!record.isEmpty())
				prepareNewFile(this.messagesFile);

		} finally {
			unlock(lockFile);
		}
		return message;
	}

	private File createTempFile() {
		File tempFile = new File(this.queueFolderName + "/temp");
		try {
			if (!tempFile.exists())
				tempFile.createNewFile();
		} catch (IOException e) {
			LOG.log(Level.SEVERE, "Failed to create the file [" + tempFile.getPath() + "].", e);
			throw new RuntimeException("Failed to create file [" + tempFile.getPath() + "].");
		}
		return tempFile;
	}

	private File getLockFile(String queueFolder) {
		return new File(queueFolder + "/.lock/");
	}

	private void lock(File lock) {
		while (!lock.mkdir()) {
			try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				LOG.log(Level.WARNING, "The thread [" + Thread.currentThread().getName() + "] got interrupted.");
			}

		}
	}

	private void unlock(File lock) {
		lock.delete();
	}

	private Optional<String> readFirstLine(File file) {
		FileReader reader = null;
		BufferedReader br = null;
		try {
			reader = new FileReader(file);
			br = new BufferedReader(reader);
			String line = br.readLine();
			return Optional.ofNullable(line);
		} catch (IOException ex) {
			LOG.log(Level.SEVERE,
					"Exception occurred while reading first " + "line from file [" + file.getPath() + "].", ex);
		} finally {
			try {
				br.close();
			} catch (IOException ignore) {
			}
			try {
				reader.close();
			} catch (IOException ignore) {
			}
		}
		return Optional.empty();
	}

	private void copyFile(File source, File target, boolean skipFirstLine) {
		BufferedReader br = null;
		PrintWriter pw = null;
		try {
			br = new BufferedReader(new FileReader(source));
			pw = new PrintWriter(new FileWriter(target));

			if (skipFirstLine)
				br.readLine();

			String line;
			while ((line = br.readLine()) != null) {
				pw.println(line);
			}
		} catch (IOException ex) {
			LOG.log(Level.SEVERE, "Exception occurred while copying the contents from file [" + source.getPath()
					+ "] to [" + target.getPath() + "].", ex);
		} finally {
			try {
				br.close();
			} catch (IOException ignore) {
			}
			pw.close();
		}
	}

	private void prepareNewFile(File messagesFile) {
		File tempFile = createTempFile();
		copyFile(messagesFile, tempFile, true);
		String messagesFilePath = messagesFile.getPath();
		messagesFile.delete();
		tempFile.renameTo(new File(messagesFilePath));
	}

	private Message getMessage(String record) {
		String[] dataArray = record.split(":");
		Message msg = new Message();
		msg.setMessageId(dataArray[0]);
		msg.setReceiptHandle(dataArray[1]);
		msg.setBody(dataArray[2]);
		return msg;
	}

	private String getMessageString(T message) {
		StringBuilder builder = new StringBuilder();
		builder.append(((Message) message).getMessageId()).append(":").append(((Message) message).getReceiptHandle())
				.append(":").append(((Message) message).getBody());
		return builder.toString();
	}

	private void writeMessage(T message) {
		try (PrintWriter pw = new PrintWriter(new FileWriter(this.messagesFile, true))) {
			pw.println(getMessageString(message));
		} catch (IOException e) {
			LOG.log(Level.SEVERE, "Failed to write the message [" + message + "] to queue file ["
					+ this.messagesFile.getPath() + "]");
			throw new RuntimeException(
					"Failed to write the message [" + message + "] to queue file [" + this.messagesFile.getPath() + "]",
					e);
		}
	}

	// To make implementation simple, the following methods are unsupported. Can
	// be implemented as per requirements

	@Override
	public void addFirst(T message) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T takeFirst() throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T takeLast() throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean isEmpty() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public int remainingCapacity() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public int drainTo(Collection<? super T> c) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public int drainTo(Collection<? super T> c, int maxElements) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T removeFirst() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T removeLast() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T pollLast() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T getFirst() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T getLast() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T peekFirst() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T peekLast() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T pop() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public Iterator<T> descendingIterator() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public void addLast(T e) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public void putFirst(T e) throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public void putLast(T e) throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean offerFirst(T e, long timeout, TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean offerLast(T e, long timeout, TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T pollLast(long timeout, TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean removeFirstOccurrence(Object o) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean removeLastOccurrence(Object o) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean add(T e) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean offer(T e) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public void put(T e) throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T remove() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T poll() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T take() throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T element() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public T peek() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public int size() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public Iterator<T> iterator() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public void push(T e) {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

}
