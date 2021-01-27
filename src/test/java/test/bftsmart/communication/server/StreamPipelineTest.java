package test.bftsmart.communication.server;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import utils.security.RandomUtils;

public class StreamPipelineTest {

	@Test
	public void testInputOutput() throws IOException, InterruptedException {
		StreamPipeline pipeline = new StreamPipeline();
		DataOutputStream output = pipeline.getOutputStream();
		DataInputStream input = pipeline.getInputStream();

		// 测试在同步环境下，写入和读取的一致性；
		{
			ByteArrayOutputStream randomBytesBuff = new ByteArrayOutputStream();
			for (int i = 0; i < 2; i++) {
				byte[] bytes = RandomUtils.generateRandomBytes(4 * i + 6);
				output.write(bytes);
				randomBytesBuff.write(bytes);
			}
			output.flush();

			byte[] totalBytes = randomBytesBuff.toByteArray();

			ByteArrayOutputStream readBuffer = new ByteArrayOutputStream();
			int totalSize = totalBytes.length;
			byte[] bf = new byte[16];
			int len = 0;
			while ((len = input.read(bf)) > 0) {
				readBuffer.write(bf, 0, len);
				totalSize -= len;
				if (totalSize <= 0) {
					break;
				}
			}
			byte[] totalReadBytes = readBuffer.toByteArray();

			assertArrayEquals(totalBytes, totalReadBytes);
		}

		// 测试在异步环境下，写入和读取的一致性；
//		{
//			CyclicBarrier barrier = new CyclicBarrier(2);
//
//			ByteArrayOutputStream randomBytesBuff = new ByteArrayOutputStream();
//			Thread sendThrd = new Thread(new Runnable() {
//				@Override
//				public void run() {
//					try {
//						barrier.await();
//					} catch (InterruptedException | BrokenBarrierException e1) {
//						e1.printStackTrace();
//					}
//					try {
//
//						for (int i = 0; i < 10; i++) {
//							byte[] bytes = RandomUtils.generateRandomBytes(4 * i + 6);
//							output.write(bytes);
//							randomBytesBuff.write(bytes);
//						}
//						output.flush();
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				}
//			});
//
//			boolean[] flag = { true };
//
//			ByteArrayOutputStream readBuffer = new ByteArrayOutputStream();
//			Thread recvThrd = new Thread(new Runnable() {
//				@Override
//				public void run() {
//					try {
//						barrier.await();
//					} catch (InterruptedException | BrokenBarrierException e1) {
//						e1.printStackTrace();
//					}
//					while (flag[0]) {
//						try {
//							byte[] bf = new byte[16];
//							int len = 0;
//							while ((len = input.read(bf)) > 0) {
//								readBuffer.write(bf, 0, len);
//							}
//						} catch (IOException e) {
//							e.printStackTrace();
//						}
//
//						try {
//							Thread.sleep(1000);
//						} catch (InterruptedException e) {
//						}
//					}
//				}
//			});
//
//			sendThrd.start();
//			recvThrd.start();
//
//			sendThrd.join();
//			Thread.sleep(1000);
//			flag[0] = false;
//			recvThrd.join();
//
//			byte[] totalBytes = randomBytesBuff.toByteArray();
//			byte[] totalReadBytes = readBuffer.toByteArray();
//
//			assertTrue(totalBytes.length > 0);
//			assertArrayEquals(totalBytes, totalReadBytes);
//		}
	}
}

