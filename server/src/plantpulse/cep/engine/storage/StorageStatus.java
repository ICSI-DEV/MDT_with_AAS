package plantpulse.cep.engine.storage;

/**
 * StorageStatus
 * @author leesa
 *
 */
public class StorageStatus {

	private long active_timer = 0;
	private long active_batch = 0;
	private long process_current_data = 0;
	private long process_total_data = 0;
	private long total_saved_tag_data_count = 0;
	private double write_per_sec_thread = 0;
	private long pending_data_size_in_buffer = 0;

	public long getActive_timer() {
		return active_timer;
	}

	public void setActive_timer(long active_timer) {
		this.active_timer = active_timer;
	}

	public long getActive_batch() {
		return active_batch;
	}

	public void setActive_batch(long active_batch) {
		this.active_batch = active_batch;
	}

	public long getProcess_total_data() {
		return process_total_data;
	}

	public void setProcess_total_data(long process_total_data) {
		this.process_total_data = process_total_data;
	}

	public double getWrite_per_sec_thread() {
		return write_per_sec_thread;
	}

	public void setWrite_per_sec_thread(double write_per_sec_thread) {
		this.write_per_sec_thread = write_per_sec_thread;
	}

	public long getPending_data_size_in_buffer() {
		return pending_data_size_in_buffer;
	}

	public void setPending_data_size_in_buffer(long pending_data_size_in_buffer) {
		this.pending_data_size_in_buffer = pending_data_size_in_buffer;
	}

	public long getProcess_current_data() {
		return process_current_data;
	}

	public void setProcess_current_data(long process_current_data) {
		this.process_current_data = process_current_data;
	}

	public long getTotal_saved_tag_data_count() {
		return total_saved_tag_data_count;
	}

	public void setTotal_saved_tag_data_count(long total_saved_tag_data_count) {
		this.total_saved_tag_data_count = total_saved_tag_data_count;
	}

}
