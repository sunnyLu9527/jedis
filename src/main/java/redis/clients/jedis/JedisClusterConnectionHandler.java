package redis.clients.jedis;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisConnectionException;

public abstract class JedisClusterConnectionHandler implements Closeable {
  protected final JedisClusterInfoCache cache;

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
                                       final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, password, null);
  }

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
          final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, String clientName) {
    this.cache = new JedisClusterInfoCache(poolConfig, connectionTimeout, soTimeout, password, clientName);
    initializeSlotsCache(nodes, poolConfig, connectionTimeout, soTimeout, password, clientName);
}

  abstract Jedis getConnection();

  abstract Jedis getConnectionFromSlot(int slot);

    /**
     * 通过HostAndPort获取缓存的Jedis实例
     * @param node
     * @return
     */
  public Jedis getConnectionFromNode(HostAndPort node) {
    return cache.setupNodeIfNotExist(node).getResource();
  }

    /**
     * 获取node-pool映射缓存
     * @return
     */
  public Map<String, JedisPool> getNodes() {
    return cache.getNodes();
  }

    /**
     * 初始化缓存映射
     * @param startNodes
     * @param poolConfig
     * @param connectionTimeout
     * @param soTimeout
     * @param password
     * @param clientName
     */
  private void initializeSlotsCache(Set<HostAndPort> startNodes, GenericObjectPoolConfig poolConfig,
                                    int connectionTimeout, int soTimeout, String password, String clientName) {
    for (HostAndPort hostAndPort : startNodes) {
      Jedis jedis = null;
      try {
        jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), connectionTimeout, soTimeout);
        if (password != null) {
          jedis.auth(password);
        }
        if (clientName != null) {
          jedis.clientSetname(clientName);
        }
        cache.discoverClusterNodesAndSlots(jedis);//建立node-pool映射缓存 slot-pool映射缓存
        break;
      } catch (JedisConnectionException e) {
        // try next nodes
      } finally {
        if (jedis != null) {
          jedis.close();
        }
      }
    }
  }

    /**
     * 重置缓存映射
     */
  public void renewSlotCache() {
    cache.renewClusterSlots(null);
  }

    /**
     * 重置缓存映射
     * @param jedis
     */
  public void renewSlotCache(Jedis jedis) {
    cache.renewClusterSlots(jedis);
  }

  @Override
  public void close() {
    cache.reset();//清空缓存；释放pool
  }
}
