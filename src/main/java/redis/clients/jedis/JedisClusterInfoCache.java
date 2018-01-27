package redis.clients.jedis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

public class JedisClusterInfoCache {
  private final Map<String, JedisPool> nodes = new HashMap<String, JedisPool>();
  private final Map<Integer, JedisPool> slots = new HashMap<Integer, JedisPool>();

  private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
  private final Lock r = rwl.readLock();
  private final Lock w = rwl.writeLock();
  private volatile boolean rediscovering;
  private final GenericObjectPoolConfig poolConfig;

  private int connectionTimeout;
  private int soTimeout;
  private String password;
  private String clientName;

  private static final int MASTER_NODE_INDEX = 2;//主节点所在的索引

  public JedisClusterInfoCache(final GenericObjectPoolConfig poolConfig, int timeout) {
    this(poolConfig, timeout, timeout, null, null);
  }

  public JedisClusterInfoCache(final GenericObjectPoolConfig poolConfig,
      final int connectionTimeout, final int soTimeout, final String password, final String clientName) {
    this.poolConfig = poolConfig;
    this.connectionTimeout = connectionTimeout;
    this.soTimeout = soTimeout;
    this.password = password;
    this.clientName = clientName;
  }

    /**
     * 建立缓存 node-pool映射缓存 slot-pool映射缓存
     * @param jedis
     */
  public void discoverClusterNodesAndSlots(Jedis jedis) {
    w.lock();

    try {
      reset();
      List<Object> slots = jedis.clusterSlots();//内部通过cluster nodes指令获取集群信息

      for (Object slotInfoObj : slots) {
          //这是一组相同主节点下的一段连续的slot的信息 slotInfo[0]代表slot起始 slotInfo[1]代表slot截止
          //slotInfo[2]代表slot所在主节点信息 slotInfo[...]代表从节点信息
        List<Object> slotInfo = (List<Object>) slotInfoObj;

        if (slotInfo.size() <= MASTER_NODE_INDEX) {
          continue;
        }

        List<Integer> slotNums = getAssignedSlotArray(slotInfo);//获取这一组连续slot的集合

        // hostInfos
        int size = slotInfo.size();
        for (int i = MASTER_NODE_INDEX; i < size; i++) {
            //这是一个主节点/从节点的集合信息hostInfos[0]代表host hostInfos[1]代表port hostInfos[2]代表nodeId
          List<Object> hostInfos = (List<Object>) slotInfo.get(i);
          if (hostInfos.size() <= 0) {
            continue;
          }

          HostAndPort targetNode = generateHostAndPort(hostInfos);//生成节点host+port
          setupNodeIfNotExist(targetNode);//node-pool映射缓存
          if (i == MASTER_NODE_INDEX) {
            assignSlotsToNode(slotNums, targetNode);//slot-pool映射缓存
          }
        }
      }
    } finally {
      w.unlock();
    }
  }

    /**
     * 重置slot-pool映射缓存
     * @param jedis
     */
  public void renewClusterSlots(Jedis jedis) {
    //If rediscovering is already in process - no need to start one more same rediscovering, just return
    if (!rediscovering) {
      try {
        w.lock();
        rediscovering = true;

        if (jedis != null) {
          try {
            discoverClusterSlots(jedis);
            return;
          } catch (JedisException e) {
            //try nodes from all pools
          }
        }

        for (JedisPool jp : getShuffledNodesPool()) {
          try {
            jedis = jp.getResource();
            discoverClusterSlots(jedis);
            return;
          } catch (JedisConnectionException e) {
            // try next nodes
          } finally {
            if (jedis != null) {
              jedis.close();
            }
          }
        }
      } finally {
        rediscovering = false;
        w.unlock();
      }
    }
  }

    /**
     * 重置slot-pool映射缓存
     * @param jedis
     */
  private void discoverClusterSlots(Jedis jedis) {
    List<Object> slots = jedis.clusterSlots();
    this.slots.clear();

    for (Object slotInfoObj : slots) {
      List<Object> slotInfo = (List<Object>) slotInfoObj;

      if (slotInfo.size() <= MASTER_NODE_INDEX) {
        continue;
      }

      List<Integer> slotNums = getAssignedSlotArray(slotInfo);

      // hostInfos
      List<Object> hostInfos = (List<Object>) slotInfo.get(MASTER_NODE_INDEX);
      if (hostInfos.isEmpty()) {
        continue;
      }

      // at this time, we just use master, discard slave information
      HostAndPort targetNode = generateHostAndPort(hostInfos);
      assignSlotsToNode(slotNums, targetNode);
    }
  }

    /**
     * 生成节点host+port
     * @param hostInfos
     * @return
     */
  private HostAndPort generateHostAndPort(List<Object> hostInfos) {
    return new HostAndPort(SafeEncoder.encode((byte[]) hostInfos.get(0)),
        ((Long) hostInfos.get(1)).intValue());
  }

    /**
     * node-pool映射缓存
     * @param node
     * @return
     */
  public JedisPool setupNodeIfNotExist(HostAndPort node) {
    w.lock();
    try {
      String nodeKey = getNodeKey(node);
      JedisPool existingPool = nodes.get(nodeKey);
      if (existingPool != null) return existingPool;

      JedisPool nodePool = new JedisPool(poolConfig, node.getHost(), node.getPort(),
          connectionTimeout, soTimeout, password, 0, clientName, false, null, null, null);
      nodes.put(nodeKey, nodePool);
      return nodePool;
    } finally {
      w.unlock();
    }
  }

    /**
     * 将slot缓存映射到指定节点所属的pool
     * @param slot
     * @param targetNode
     */
  public void assignSlotToNode(int slot, HostAndPort targetNode) {
    w.lock();
    try {
      JedisPool targetPool = setupNodeIfNotExist(targetNode);
      slots.put(slot, targetPool);
    } finally {
      w.unlock();
    }
  }

    /**
     * slot-pool映射缓存
     * @param targetSlots
     * @param targetNode
     */
  public void assignSlotsToNode(List<Integer> targetSlots, HostAndPort targetNode) {
    w.lock();
    try {
      JedisPool targetPool = setupNodeIfNotExist(targetNode);
      for (Integer slot : targetSlots) {
        slots.put(slot, targetPool);
      }
    } finally {
      w.unlock();
    }
  }

    /**
     * 从缓存中通过nodeKey获取JedisPool
     * @param nodeKey hnp.getHost() + ":" + hnp.getPort()  见getNodeKey(HostAndPort hnp)
     * @return
     */
  public JedisPool getNode(String nodeKey) {
    r.lock();
    try {
      return nodes.get(nodeKey);
    } finally {
      r.unlock();
    }
  }

    /**
     * 从缓存中通过slot获取JedisPool
      * @param slot
     * @return
     */
  public JedisPool getSlotPool(int slot) {
    r.lock();
    try {
      return slots.get(slot);
    } finally {
      r.unlock();
    }
  }

    /**
     * 获取node-pool映射缓存
     * @return
     */
  public Map<String, JedisPool> getNodes() {
    r.lock();
    try {
      return new HashMap<String, JedisPool>(nodes);
    } finally {
      r.unlock();
    }
  }

  public List<JedisPool> getShuffledNodesPool() {
    r.lock();
    try {
      List<JedisPool> pools = new ArrayList<JedisPool>(nodes.values());
      Collections.shuffle(pools);//打乱顺序
      return pools;
    } finally {
      r.unlock();
    }
  }

  /**
   * 清空缓存；释放pool
   * Clear discovered nodes collections and gently release allocated resources
   */
  public void reset() {
    w.lock();
    try {
      for (JedisPool pool : nodes.values()) {
        try {
          if (pool != null) {
            pool.destroy();
          }
        } catch (Exception e) {
          // pass
        }
      }
      nodes.clear();
      slots.clear();
    } finally {
      w.unlock();
    }
  }

    /**
     * 通过HostAndPort拼接节点host:port
     * @param hnp
     * @return
     */
  public static String getNodeKey(HostAndPort hnp) {
    return hnp.getHost() + ":" + hnp.getPort();
  }

    /**
     * 通过Client拼接节点host:port
     * @param client
     * @return
     */
  public static String getNodeKey(Client client) {
    return client.getHost() + ":" + client.getPort();
  }

    /**
     * 通过Jedis拼接节点host:port
     * @param jedis
     * @return
     */
  public static String getNodeKey(Jedis jedis) {
    return getNodeKey(jedis.getClient());
  }

    /**
     * 获取一组连续slot的集合
     * @param slotInfo
     * @return
     */
  private List<Integer> getAssignedSlotArray(List<Object> slotInfo) {
    List<Integer> slotNums = new ArrayList<Integer>();
    for (int slot = ((Long) slotInfo.get(0)).intValue(); slot <= ((Long) slotInfo.get(1))
        .intValue(); slot++) {
      slotNums.add(slot);
    }
    return slotNums;
  }
}
