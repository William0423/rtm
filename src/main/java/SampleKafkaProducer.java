
import java.util.*;
import java.io.*;

import com.google.protobuf.InvalidProtocolBufferException;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import zampdata.records.pb.MegatronLog.*;

public class SampleKafkaProducer {

	public static void sendRecords(Producer<String, byte[]> producer,
								   String topic, String datafile, int sleepInterval,
								   TimeDistiller td)
								   throws InterruptedException {
		try {
			FileInputStream fis = new FileInputStream(datafile);
	
			byte[] buf = new byte[4];
			int accuSleep = 0;
			int count = 0;
			long startTime = System.currentTimeMillis();
			long logStartTime = -1;
			long logEndTime = -1;
			while (fis.read(buf) == 4) {
				int len = ((int)(buf[0] & 0xFF) << 24) +
						  ((int)(buf[1] & 0xFF) << 16) + 
						  ((int)(buf[2] & 0xFF) << 8) +
						  (int)(buf[3] & 0xFF);

				byte[] protodata = new byte[len];
				if (fis.read(protodata) != len) break;

				String key = System.currentTimeMillis() + "";
				KeyedMessage<String, byte[]> record =
					new KeyedMessage<String, byte[]>(topic, key, protodata);
				producer.send(record);

				int b;
				do {
					b = fis.read(buf, 0, 1);
				} while (buf[0] != 10 && b != -1);

/*
				accuSleep += sleepInterval;
				if (accuSleep >= 1000) {
					Thread.sleep(0, accuSleep / 1000);
					accuSleep %= 1000;
				}
*/
				long time = td.getTime(protodata);
				if (logStartTime < 0) {
					logStartTime = time;
				} else {
					long logElapsedTime = time - logStartTime;
					long elapsedTime = System.currentTimeMillis() - startTime;

					if  (logElapsedTime - elapsedTime > 10) {
						Thread.sleep(logElapsedTime - elapsedTime);
					}
				}
				logEndTime = time;
				++count;
			}
			System.out.println("read " + count + " records within " + (System.currentTimeMillis() - startTime) + "ms.");
			System.out.println("log span " + (logEndTime - logStartTime) + "ms.");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
		}
	}

	interface TimeDistiller {
		public long getTime(byte[] data) throws InvalidProtocolBufferException;
	}

	static class MegatronTimeDistiller implements TimeDistiller {

		public long getTime(byte[] data) throws InvalidProtocolBufferException {
			Megatornlogs megatron = Megatornlogs.parseFrom(data);
			long time = 0;
			if (megatron.getLogsList().size() > 0) {
				time = megatron.getLogsList().get(0).getRequestTime() * 1000;
			}
			return time;
		}
	}

	static class BillingTimeDistiller implements TimeDistiller {

		public long getTime(byte[] data) throws InvalidProtocolBufferException {
			BillingLog.billing_log billing = BillingLog.billing_log.parseFrom(data);
			long time = 0;
			String timestr = billing.getRequestTime();
			String[] hms = timestr.split(":");

			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.HOUR, Integer.parseInt(hms[0]));
			cal.set(Calendar.MINUTE, Integer.parseInt(hms[1]));
			cal.set(Calendar.SECOND, Integer.parseInt(hms[2]));

			return cal.getTimeInMillis();
		}
	}

	public static void main(String[] args) {		
		if (args.length != 5) {
			System.out.println("Usage: SampleKafkaProducer datafile sleepInterval broker topic {TYPE}.");
			System.out.println("- {TYPE} [0|1]; 0 for megatron, 1 for billing.");
			System.exit(1);
		}

		String datafile = args[0];
		int sleepInterval = Integer.parseInt(args[1]);
		String broker = args[2];
		String topic = args[3];

		TimeDistiller td = null;
		if (args[4].equals("0")) {
			td = new MegatronTimeDistiller();
		} else {
			td = new BillingTimeDistiller();
		}

		Properties props = new java.util.Properties();
		props.put("metadata.broker.list", broker);
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

		System.out.println("read from " + datafile + " with interval " + sleepInterval + " milliseconds.");

		try {
			while (true) {
				sendRecords(producer, topic, datafile, sleepInterval, td);
				System.out.println("repeat again");
			}
		} catch (InterruptedException e) {
		} finally {
			producer.close();
			System.out.println("close producer.");
		}
	}
}
