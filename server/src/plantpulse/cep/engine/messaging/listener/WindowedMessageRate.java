package plantpulse.cep.engine.messaging.listener;

/**
 * WindowedMessageRate
 * 
 * @author leesa
 *
 */
public class WindowedMessageRate {

	private double normalizedRate; // event rate / window
	private long windowSizeTicks;
	private long lastEventTicks;

	/**
	 * WindowedMessageRate
	 * @param aWindowSizeSeconds (초)
	 */
	public WindowedMessageRate(int aWindowSizeSeconds) {
		windowSizeTicks = aWindowSizeSeconds * 1000L;
		lastEventTicks = System.currentTimeMillis();
	}

	/**
	 * newMessage
	 * @return
	 */
	public double newMessage() {

		long currentTicks = System.currentTimeMillis();
		long period = currentTicks - lastEventTicks;
		lastEventTicks = currentTicks;
		double normalizedFrequency = (double) windowSizeTicks / (double) period;

		double alpha = Math.min(1.0 / normalizedFrequency, 1.0);
		normalizedRate = (alpha * normalizedFrequency) + ((1.0 - alpha) * normalizedRate);
		return getRate();
	}

	public double getRate() {
		return normalizedRate * 1000L / windowSizeTicks;
	}
}