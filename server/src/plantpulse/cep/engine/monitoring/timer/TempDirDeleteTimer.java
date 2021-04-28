package plantpulse.cep.engine.monitoring.timer;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.engine.timer.TimerExecuterPool;

/**
 * TempDirDeleteTimer
 * 
 * <pre>
 *   /java.io.tmpdir/plantpulse/ 내에 생성된 임시파일들을 삭제합니다.
 *   - ARRF 파일 (머신러닝용)
 *   - CSV 다운로드용 생성 파일
 * </pre>
 * 
 * @author lsb
 *
 */
public class TempDirDeleteTimer  implements MonitoringTimer {

	private static final Log log = LogFactory.getLog(TempDirDeleteTimer.class);

	private static final int TIMER_PERIOD = (1000 * 60 * 60 * 24); // 24시간 마다 실행

	private ScheduledFuture<?> task;

	public void start() {
		//
		Calendar date = Calendar.getInstance();
		date.setTime(new Date());
		date.set(Calendar.HOUR, 23);
		date.set(Calendar.MINUTE, 59);
		date.set(Calendar.SECOND, 59);
		
		//
		task = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {

				//
				try {
					cleanTempDir();
					//

				} catch (Exception e) {
					log.error(e);
				}

			}

		}, (date.getTime().getTime() - System.currentTimeMillis()) + 601, TIMER_PERIOD, TimeUnit.MILLISECONDS); //12시 10분부터

		log.info("JAVA Temp directory cleaner timer started.");

		// First
		cleanTempDir();
	}

	public void stop() {
		task.cancel(true);
	};

	private static void cleanTempDir() {
		File temp = new File(System.getProperty("java.io.tmpdir") + "/plantpulse");
		long size = getFileFolderSize(temp);
		FileUtils.deleteQuietly(temp);
		temp.mkdirs();

		log.info("JAVA Temp directory deleted : dir=[" + temp.getAbsolutePath() + "], size=[" + humanReadableByteCount(size, true) + "]");
	}

	public static long getFileFolderSize(File dir) {
		long size = 0;
		if (dir.isDirectory()) {
			for (File file : dir.listFiles()) {
				if (file.isFile()) {
					size += file.length();
				} else
					size += getFileFolderSize(file);
			}
		} else if (dir.isFile()) {
			size += dir.length();
		}
		return size;
	}

	private static String humanReadableByteCount(long bytes, boolean si) {
		int unit = si ? 1000 : 1024;
		if (bytes < unit)
			return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	};

}
