package com.orientechnologies.orient.test.internal.io;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;

@Test
public class FileRangeLockSpeedTest {
	private File file;
	private RandomAccessFile randomAccessFile;
	private FileChannel fileChannel;
	
	private final static int MAX_ITERATIONS = 1000;
	private final static int FILE_SIZE = 1024 * 1024;
	private final static int LOCK_CHUNK_SIZE = 1024;
	

	@BeforeClass
	public void setUp() throws IOException {
		file = new File("fileLockSpeedTest");
		file.createNewFile();
		
		randomAccessFile = new RandomAccessFile(file, "rw");
		fileChannel = randomAccessFile.getChannel();
		
		final ByteBuffer byteBuffer = ByteBuffer.allocate(FILE_SIZE);
		for(int i = 0; i < FILE_SIZE; i++) {
			byteBuffer.put((byte)1);
		}
		
		fileChannel.write(byteBuffer);
		fileChannel.force(true);
	}
	
	@AfterClass
	public void tearDown() throws IOException {
		fileChannel.close();
		randomAccessFile.close();
		
		file.delete();
	}
	
	public void testLockSpeed() throws IOException {
		final List<FileLock> fileLocks = new ArrayList<FileLock>(1000);
		
		final long sharedLockStartTime = System.currentTimeMillis();

		for(int i  = 0; i < MAX_ITERATIONS; i++) {
			for (int n = 0; n < FILE_SIZE / LOCK_CHUNK_SIZE; n++ ) {
				fileLocks.add(fileChannel.lock(n * LOCK_CHUNK_SIZE, LOCK_CHUNK_SIZE , true));
			}
			
			for(FileLock fileLock : fileLocks) {
				fileLock.release();
			}
		}

		final long sharedLockEndTime = System.currentTimeMillis();

		System.out.println("Shared locks : " +
						((sharedLockEndTime - sharedLockStartTime) / (FILE_SIZE / LOCK_CHUNK_SIZE)) + " ms. per lock/unlock.");
		fileLocks.clear();

		final long exclusiveLockStartTime = System.currentTimeMillis();

		for(int i  = 0; i < MAX_ITERATIONS; i++) {
			for (int n = 0; n < FILE_SIZE / LOCK_CHUNK_SIZE; n++ ) {
				fileLocks.add(fileChannel.lock(n * LOCK_CHUNK_SIZE, LOCK_CHUNK_SIZE , false));
			}

			for(FileLock fileLock : fileLocks) {
				fileLock.release();
			}
		}

		final long exclusiveLockEndTime = System.currentTimeMillis();

		System.out.println("Exclusive locks : " +
						((exclusiveLockEndTime - exclusiveLockStartTime) / (FILE_SIZE / LOCK_CHUNK_SIZE)) + " ms. per lock/unlock.");

	}
	
}
