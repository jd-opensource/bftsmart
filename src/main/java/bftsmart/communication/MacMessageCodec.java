package bftsmart.communication;

/**
 * 带消息认证的消息编解码器；
 * @author huanghaiquan
 *
 * @param <T>
 */
public interface MacMessageCodec<T> extends MessageCodec<T>{
	
	MacKey getMacKey();
	
}
