package plantpulse.cep.engine.storage;

/**
 * 테이블의 생명 주기 TTL (초)
 * 
 * @author lsb
 *
 */
public class StorageTTL {
	
	private static final int DAY_UNIT = 86400; //1일
	
	//
	public static final int TAG_POINT_TABLE_TTL                   = DAY_UNIT * 14; // 포인트 원본 14일 저장
	
	public static final int TAG_BLOB_TABLE_TTL                    = DAY_UNIT * 31; // 포인트 원본 1달 저장
	
	public static final int TAG_POINT_BUCKET_TABLE_TTL            = DAY_UNIT * 90; // 포인트 원본 3달 저장 (버켓 3주)
	
	public static final int TAG_POINT_LIMIT_1_DAYS_TABLE_TTL      = DAY_UNIT  * 1;      // 포인트 제한 7일 저장
	
	public static final int TAG_POINT_LIMIT_7_DAYS_TABLE_TTL      = DAY_UNIT  * 7;      // 포인트 제한 7일 저장
	
	public static final int TAG_POINT_LIMIT_15_DAYS_TABLE_TTL     = DAY_UNIT * 15;      // 포인트 제한 15일 저장
	
	public static final int TAG_POINT_LIMIT_31_DAYS_TABLE_TTL     = DAY_UNIT * 31;      // 포인트 제한 31일 저장
	
	public static final int TAG_POINT_MAP_TABLE_TTL        = DAY_UNIT * 1;    // 포인트 맵 1일 저장
	
	public static final int TAG_POINT_SNAPSHOT_TABLE_TTL   = DAY_UNIT * 31;  // 스냅샷 1달
	
	public static final int TAG_POINT_AGGREGATION_TTL      = DAY_UNIT * 90; // 집계  3달
	
	public static final int TAG_POINT_SAMPLING_TABLE_TTL   = DAY_UNIT * 90;  // 샘플링 3달
	
	public static final int TAG_POINT_ARCHIVE_TABLE_TTL    = DAY_UNIT * 365 * 1; // 아카이브 1년
	
	//
	public static final int TM_ASSET_DATA_TABLE_TTL        = DAY_UNIT * 60; // 에셋 데이터 1달
	
	
}
