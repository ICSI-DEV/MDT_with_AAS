package plantpulse.cep.engine.pipeline.dataflow.adjuster;

import plantpulse.event.opc.Point;

/**
 * DataFlowAdjuster
 * 
 * @author lsb
 *
 */
public interface DataFlowAdjuster {
	
	/**
	 * 포인트 검증
	 * 
	 * @throws Exception
	 */
	public boolean validate(Point point) throws Exception;
	
	/**
	 * 스트리밍전에 포인트 처리
	 * 
	 * @param point
	 * @throws Exception
	 */
	public Point beforeStream(Point point) throws Exception;
	
	/**
	 * 저장전에 포인트 처리
	 * 
	 * @param point
	 * @throws Exception
	 */
	public Point beforeStore(Point point) throws Exception;

	

}
