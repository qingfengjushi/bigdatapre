package com.aa.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author LIAO
 */
public class ZooKeeperDemo {
    // 客户端去请求链接的时候的服务器链接地址信息
    //private static String connectString = "hadoop10:2181,hadoop11:2181,hadoop12:2181";
    private static String connectString = "hadoop10:2181";

    // 客户端去请求链接的超时时长
    private static int sessionTimeout = 100000;
    // 节点名称，统一命名
    private static String znode = "/zk3";

    public static void main(String[] args) throws Exception {
        // 拿 ZooKeeper 链接
        ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, null);
        //System.out.println("==== " + zk + "====");
        // 创建 znode //创建的时候，这个节点不能存在，存在会报错。
        String createdNode = zk.create(znode, "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(createdNode+"节点创建成功");

        // 查看节点
        //byte[] data = zk.getData("/hbase/meta-region-server", null, null);
        //打印出来了hello节点了。
        //System.out.println(new String(data));

        // 修改节点数据 为hello2
        /*Stat setData = zk.setData(znode, "hello2".getBytes(), -1);
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(setData.getMtime())));
        byte[] data1 = zk.getData(znode, null, null);
        System.out.println(new String(data1));*/


        // 判断节点是否存在
        /*Stat exists = zk.exists(znode, null);
        System.out.println(null != exists?true:false);*/

        // 获取子节点
        /*List<String> children = zk.getChildren("/hbase", null);
        for(String child: children){
            System.out.println(child);
        }*/
        // 删除节点
        /*zk.delete(znode, -1);
        Stat exists1 = zk.exists(znode, null);
        System.out.println(null == exists1?"删除成功":"删除失败");*/

        zk.close();
    }
}
