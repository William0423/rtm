
import java.util.*;
import java.io.*;
import java.text.SimpleDateFormat;

import com.google.protobuf.InvalidProtocolBufferException;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import tracking.Tracking.*;

public  class TrackingTestProducer {

/*
2015-01-23T14:00:01+08:00	101.31.61.162	200	/imp/i3	fef325696cc505c88937c9688c95dd05	p=23&mk=Y1vXDdgs_xLsD8CWtU1pxFaQKnmWG8iQmEv1M_dmtRwJ4t0ZrkcpiXAMo6KWE_VOr5HhWDbtxcgunZ2zPS819rUcwtPFN
gj4qCob2m1Mq5hwDKOilhP1Tk9o1CbodnCKp3PvdfKJ55iwJZYVv20P2Zuaagt9wCyQLvnK2APqnYwCGyqTpziLtBRyuczHT8dBPZfQremHO_hZgFHmb-ozpfpj9AJ8Hp4cQAc389zYep4I3HHFxalSafmi0rhyKGDbajkrWewaASw_HhDFpiwj0qJDlEFlWSZTWswiyW301K
cQ0BzEGZHHuIMT0vN8rbD4jwoYKTQW1t6-n8jWgUzObQy_pUUc0hlmQPSqDgI12X50oW8KI90zKmhHxVRDe5s_Q0knYyBterwOG2w5CXpQp6ItwG8Esm1i&t=9	-	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) C
hrome/30.0.1599.101 Safari/537.36	0.049	http://cdn.zampdsp.com/v3/static/scripts/tploader3_1.htm
*/

	public static SimpleDateFormat datefmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
	public static String[] aURIs = { "/imp/i3", "/tms/c3" };
	public static HashSet<String> URIs = new HashSet<String>();
	static {
		for (String u : aURIs) URIs.add(u);
		System.out.println(URIs.size());
	}

	public static void sendRecords(Producer<String, byte[]> producer,
								   String topic, String datafile, int sleepInterval)
								   throws InterruptedException {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(datafile));
	
			int count = 0;
			int badcount = 0;
			long startTime = System.currentTimeMillis();
			long logStartTime = -1;
			long logEndTime = -1;

			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] fields = line.split("\t");

				try {
					long time = datefmt.parse(fields[0]).getTime();
					String ip = fields[1];
					int status = Integer.parseInt(fields[2]);
					String uri = fields[3];
					String zid = fields[4];
					String param = fields[5];
					String ua = fields[7];
					int processTime = Math.round(Float.parseFloat(fields[8]) * 1000);
					String referer = fields[9];

					common_log cl = common_log.newBuilder()
						.setRequestTime(time / 1000)
						.setIp(0xFFFFFFFF)
						.setStatus(status)
						.setUri(uri)
						.setZid(zid)
						.setParam(param)
						.setBody("")
						.setUa(ua)
						.setProcessTime(processTime)
						.setRefer(referer)
						.build();

					if (!URIs.contains(uri)) continue;

					ByteArrayOutputStream bs = new java.io.ByteArrayOutputStream();
					cl.writeTo(bs);

					byte[] protodata = bs.toByteArray();

					String key = System.currentTimeMillis() + "";
					KeyedMessage<String, byte[]> record =
						new KeyedMessage<String, byte[]>(topic, key, protodata);
					producer.send(record);

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
				} catch (NumberFormatException e) {
					++badcount;
				} catch (java.text.ParseException pe) {
					++badcount;
				} catch (ArrayIndexOutOfBoundsException e) {
					++badcount;
				}
			}
			System.out.println("read " + count + " records, " + badcount + "bad counts within " + (System.currentTimeMillis() - startTime) + "ms.");
			System.out.println("log span " + (logEndTime - logStartTime) + "ms.");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
		}
	}

	public static void main(String[] args) {
		if (args.length != 5) {
			System.out.println("Usage: TrackingTestProducer datafile sleepInterval broker topic.");
			System.exit(1);
		}
		String datafile = args[0];
		int sleepInterval = Integer.parseInt(args[1]);
		String broker = args[2];
		String topic = args[3];

		Properties props = new java.util.Properties();
		props.put("metadata.broker.list", broker);
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

		System.out.println("read from " + datafile + " with interval " + sleepInterval + " milliseconds.");

		try {
			while (true) {
				sendRecords(producer, topic, datafile, sleepInterval);
			}
		} catch (InterruptedException e) {
		} finally {
			producer.close();
			System.out.println("close producer.");
		}
	}
}
