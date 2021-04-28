package plantpulse.cep.engine.monitoring.event;

/**
 * OSPerformance
 * 
 * @author lsb
 * 
 */
public class OSPerformance {

	private double free_cpu_percent = 0;
	private double used_cpu_percent = 0;

	private double free_memory_percent = 0;
	private double used_memory_percent = 0;

	private double total_memory_size = 0;
	private double half_memory_size = 0;
	private double free_memory_size = 0;
	private double used_memory_size = 0;

	private int total_disk_size = 0;

	private String path_disk_name_1 = "";
	private double free_disk_size_1 = 0;
	private double total_disk_size_1 = 0;

	private String path_disk_name_2 = "";
	private long free_disk_size_2 = 0;
	private long total_disk_size_2 = 0;

	private String path_disk_name_3 = "";
	private long free_disk_size_3 = 0;
	private long total_disk_size_3 = 0;

	private String path_disk_name_4 = "";
	private long free_disk_size_4 = 0;
	private long total_disk_size_4 = 0;

	private String path_disk_name_5 = "";
	private long free_disk_size_5 = 0;
	private long total_disk_size_5 = 0;

	public double getFree_cpu_percent() {
		return free_cpu_percent;
	}

	public void setFree_cpu_percent(double free_cpu_percent) {
		this.free_cpu_percent = free_cpu_percent;
	}

	public double getUsed_cpu_percent() {
		return used_cpu_percent;
	}

	public void setUsed_cpu_percent(double used_cpu_percent) {
		this.used_cpu_percent = used_cpu_percent;
	}

	public double getFree_memory_percent() {
		return free_memory_percent;
	}

	public void setFree_memory_percent(double free_memory_percent) {
		this.free_memory_percent = free_memory_percent;
	}

	public double getUsed_memory_percent() {
		return used_memory_percent;
	}

	public void setUsed_memory_percent(double used_memory_percent) {
		this.used_memory_percent = used_memory_percent;
	}

	public double getTotal_memory_size() {
		return total_memory_size;
	}

	public void setTotal_memory_size(double total_memory_size) {
		this.total_memory_size = total_memory_size;
	}

	public double getHalf_memory_size() {
		return half_memory_size;
	}

	public void setHalf_memory_size(double half_memory_size) {
		this.half_memory_size = half_memory_size;
	}

	public double getFree_memory_size() {
		return free_memory_size;
	}

	public void setFree_memory_size(double free_memory_size) {
		this.free_memory_size = free_memory_size;
	}

	public double getUsed_memory_size() {
		return used_memory_size;
	}

	public void setUsed_memory_size(double used_memory_size) {
		this.used_memory_size = used_memory_size;
	}

	public int getTotal_disk_size() {
		return total_disk_size;
	}

	public void setTotal_disk_size(int total_disk_size) {
		this.total_disk_size = total_disk_size;
	}

	public String getPath_disk_name_1() {
		return path_disk_name_1;
	}

	public void setPath_disk_name_1(String path_disk_name_1) {
		this.path_disk_name_1 = path_disk_name_1;
	}

	public double getFree_disk_size_1() {
		return free_disk_size_1;
	}

	public void setFree_disk_size_1(double free_disk_size_1) {
		this.free_disk_size_1 = free_disk_size_1;
	}

	public double getTotal_disk_size_1() {
		return total_disk_size_1;
	}

	public void setTotal_disk_size_1(double total_disk_size_1) {
		this.total_disk_size_1 = total_disk_size_1;
	}

	public String getPath_disk_name_2() {
		return path_disk_name_2;
	}

	public void setPath_disk_name_2(String path_disk_name_2) {
		this.path_disk_name_2 = path_disk_name_2;
	}

	public long getFree_disk_size_2() {
		return free_disk_size_2;
	}

	public void setFree_disk_size_2(long free_disk_size_2) {
		this.free_disk_size_2 = free_disk_size_2;
	}

	public long getTotal_disk_size_2() {
		return total_disk_size_2;
	}

	public void setTotal_disk_size_2(long total_disk_size_2) {
		this.total_disk_size_2 = total_disk_size_2;
	}

	public String getPath_disk_name_3() {
		return path_disk_name_3;
	}

	public void setPath_disk_name_3(String path_disk_name_3) {
		this.path_disk_name_3 = path_disk_name_3;
	}

	public long getFree_disk_size_3() {
		return free_disk_size_3;
	}

	public void setFree_disk_size_3(long free_disk_size_3) {
		this.free_disk_size_3 = free_disk_size_3;
	}

	public long getTotal_disk_size_3() {
		return total_disk_size_3;
	}

	public void setTotal_disk_size_3(long total_disk_size_3) {
		this.total_disk_size_3 = total_disk_size_3;
	}

	public String getPath_disk_name_4() {
		return path_disk_name_4;
	}

	public void setPath_disk_name_4(String path_disk_name_4) {
		this.path_disk_name_4 = path_disk_name_4;
	}

	public long getFree_disk_size_4() {
		return free_disk_size_4;
	}

	public void setFree_disk_size_4(long free_disk_size_4) {
		this.free_disk_size_4 = free_disk_size_4;
	}

	public long getTotal_disk_size_4() {
		return total_disk_size_4;
	}

	public void setTotal_disk_size_4(long total_disk_size_4) {
		this.total_disk_size_4 = total_disk_size_4;
	}

	public String getPath_disk_name_5() {
		return path_disk_name_5;
	}

	public void setPath_disk_name_5(String path_disk_name_5) {
		this.path_disk_name_5 = path_disk_name_5;
	}

	public long getFree_disk_size_5() {
		return free_disk_size_5;
	}

	public void setFree_disk_size_5(long free_disk_size_5) {
		this.free_disk_size_5 = free_disk_size_5;
	}

	public long getTotal_disk_size_5() {
		return total_disk_size_5;
	}

	public void setTotal_disk_size_5(long total_disk_size_5) {
		this.total_disk_size_5 = total_disk_size_5;
	}

	@Override
	public String toString() {
		return "OSPerformance [free_cpu_percent=" + free_cpu_percent + ", used_cpu_percent=" + used_cpu_percent + ", free_memory_percent=" + free_memory_percent + ", used_memory_percent="
				+ used_memory_percent + ", total_memory_size=" + total_memory_size + ", half_memory_size=" + half_memory_size + ", free_memory_size=" + free_memory_size + ", used_memory_size="
				+ used_memory_size + "]";
	}
}
