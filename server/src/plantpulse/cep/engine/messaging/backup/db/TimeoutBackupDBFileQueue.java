package plantpulse.cep.engine.messaging.backup.db;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infobip.lib.popout.CompressedFilesConfig;
import org.infobip.lib.popout.Deserializer;
import org.infobip.lib.popout.FileQueue;
import org.infobip.lib.popout.Serializer;
import org.infobip.lib.popout.WalFilesConfig;

import io.appulse.utils.SizeUnit;
import plantpulse.cep.engine.messaging.backup.TimeoutBackupDB;
import plantpulse.cep.engine.messaging.listener.MessageListenerStatus;

/**
 * FileDBQueue
 * 
 * 메세지 브로커로 이동
 * 
 * @author leesa
 *
 */
public class TimeoutBackupDBFileQueue implements TimeoutBackupDB {

	private static final Log log = LogFactory.getLog(TimeoutBackupDBFileQueue.class);

	//public static final String DB_DIR_PATH = System.getProperty("java.io.tmpdir") + "/timeout-message/";
	public static final String DB_DIR_PATH = "/tmp/timeout-message/";
	//
	public static final String DB_FILE_PATH = DB_DIR_PATH + "/queue/";
	public static final String DB_WAL_PATH = DB_DIR_PATH + "/wal/";
	public static final String DB_COMPRESS_PATH = DB_DIR_PATH + "/compressed/";

	public static final String TYPE = "FILE"; // FILE / DIRECT_MEMORY

	public static final boolean IS_REMOVE_FILE_STARTUP = true;
	
	private FileQueue<String> queue = null;

	private AtomicLong count = new AtomicLong();

	public void init() {

		try {

			FileUtils.forceMkdir(new File(DB_FILE_PATH));
			FileUtils.forceMkdir(new File(DB_WAL_PATH));
			FileUtils.forceMkdir(new File(DB_COMPRESS_PATH));

			queue = FileQueue.<String>batched()
					// the name of the queue, used in file patterns
					.name("PP-TIMEOUT-MESSAGE-QUEUE")
					// the default folder for all queue's files
					.folder(DB_FILE_PATH)
					// sets custom serializer
					.serializer(Serializer.STRING)
					// sets custom deserializer
					.deserializer(Deserializer.STRING)
					// set up the queue's limits settings
					// .limit(QueueLimit.queueLength()
					// .length(1_000_000_000)
					// .handler(myQueueLimitExceededHandler))
					// restores from disk or not, during startup. If 'false' - the previous files
					// will be removed
					.restoreFromDisk(false)
					// handler for corrupted data from disk
					// .corruptionHandler(new MyCorruptionHandler())
					// WAL files configuration
					.wal(WalFilesConfig.builder()
							// the place where WAL files stores. Default is a queue's folder above
							.folder(DB_WAL_PATH)
							// the maximum allowed amount of WAL files before compression
							.maxCount(1000).build())
					// compressed files config
					.compressed(CompressedFilesConfig.builder()
							// the place where compressed files stores. Default is a queue's folder above
							.folder(DB_COMPRESS_PATH)
							// the maximum allowed compressed file's size
							.maxSizeBytes(SizeUnit.MEGABYTES.toBytes(256)).build())
					// the amount of elements in one WAL file. only batched queue option
					.batchSize(10_000).build();

			count.set(0);

			log.info("Timeout message backup db inited. type=[FILE_QUEUE]");

		} catch (Exception e) {
			log.error("Timeout message backup db init failed : " + e.getMessage(), e);
		}
	};

	public Object getDb() {
		return queue;
	}


	public void add(String in) {
		queue.add(in);
		MessageListenerStatus.TIMEOUT_BACKUPED_TOTAL_COUNT.incrementAndGet();
	}
	
	public String poll() {
		return queue.poll();
	}

	public File getFile() {
		return new File(DB_DIR_PATH);
	}
	
	public void close() {
		queue.close();
	}

	public String getType() {
		return TYPE;
	}
	
	public int size() {
		return queue.size();
	}
	
	public long diskSize() {
		return queue.diskSize();
	}

}
