import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by 35267 on 2021/11/6.
 */
public class HdfsTest {
    static {
        try {
            // 设置 HADOOP_HOME 目录
            System.setProperty("hadoop.home.dir", "C:\\work\\sourcecode\\hadoop-3.2.2");
            // 加载库文件
            System.load("C:\\work\\sourcecode\\hadoop-3.2.2\\bin\\hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    @Test
    public void getFileSystem1() throws IOException {
        //1:创建Configuration对象      
        Configuration conf = new Configuration();
        //2:设置文件系统类型      
        conf.set("fs.defaultFS", "hdfs://hadoop10:8020");
        //3:获取指定文件系统      
        FileSystem fileSystem = FileSystem.get(conf);
        //4:输出测试      
        System.out.println(fileSystem);
    }

    @Test
    public void getFileSystem2() throws IOException {
        //1:创建Configuration对象      
        Configuration conf = new Configuration();
        //2:设置文件系统类型      
        conf.set("fs.defaultFS", "hdfs://hadoop10:8020");
        //3:获取指定文件系统      
        FileSystem fileSystem = FileSystem.newInstance(conf);
        //4:输出测试      
        System.out.println(fileSystem);
    }

    @Test
    public void getFileSystem3() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration());
        System.out.println("fileSystem:"+fileSystem);
    }

    @Test
    public void getFileSystem4() throws Exception {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop10:8020"), new Configuration());
        System.out.println("fileSystem:"+fileSystem);
    }

    @Test
    public void listFiles() throws URISyntaxException, IOException, InterruptedException {
        //1、获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration());
        //2、调用方法listFiles 获取 /目录下所有的文件信息
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        //3、遍历迭代器
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            //获取文件的绝对路径 : hdfs://hadoop10:8020/xxx
            System.out.println(fileStatus.getPath() + "======" +fileStatus.getPath().getName());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("block数量为: "+hosts.length);
                for (String host : hosts) {
                    System.out.println("主机为: "+host);
                }
            }
            System.out.println();
        }
    }

    @Test
    public void mkdirs() throws URISyntaxException, IOException, InterruptedException {
        //1:获取FileSystem实例      
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration(),"root");
        //2:创建文件夹      
        boolean bl = fileSystem.mkdirs(new Path("/aaa/bbb/ccc"));
        System.out.println(bl);
        //3: 关闭FileSystem      
        fileSystem.close();
    }

    @Test
    public void uploadFile() throws URISyntaxException, IOException, InterruptedException {
        //1:获取FileSystem      
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration(),"root");
        //2:调用方法，实现上传      
        fileSystem.copyFromLocalFile(new Path("C://test//settings(painting).xml"), new Path("/aaa/bbb/ccc"));
        //3:关闭FileSystem      
        fileSystem.close();
    }

    @Test
    public void uploadFile2() throws IOException, InterruptedException, URISyntaxException {
        //1、获取文件系统      
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop10:8020"), configuration, "root");
        //2、上传文件操作
        /*** @param delSrc whether to delete the src 默认是不删除         *
         * @param overwrite whether to overwrite an existing file 默认是覆盖写入true         *
         * @param src path         *
         * @param dst path        
         **/
        fs.copyFromLocalFile(false,true,new Path("C:\\test\\test.txt"), new Path("/aaa/bbb/ccc"));
        //3、关闭资源
        fs.close();
    }

    @Test
    public void downloadFile1() throws URISyntaxException, IOException {
        //1:获取FileSystem      
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration());
        //2:调用方法，实现文件的下载      
        // boolean delSrc 指是否将原文件删除      
        // Path src 指要下载的文件路径      
        // Path dst 指将文件下载到的路径      
        // boolean useRawLocalFileSystem 是否开启文件校验 就是是否生成windows系统是上面那个crc文件,设置true，不会有crc文件。设置false在本地会有crc文件。
        fileSystem.copyToLocalFile(false, new Path("/aaa/bbb/ccc/test.txt"), new Path("C://test//test1_down1.txt"),false);
        //3:关闭FileSystem      
        fileSystem.close();
    }

    /**    
     * 文件的下载方式二：通过输入输出流    
     * @throws URISyntaxException    
     * @throws IOException    
     */
    @Test
    public void downloadFile2() throws URISyntaxException, IOException {
        //1:获取FileSystem      
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration());
        //2:获取hdfs的输入流      
        FSDataInputStream inputStream = fileSystem.open(new Path("/aaa/bbb/ccc/test.txt"));
        //3:获取本地路径的输出流      
        FileOutputStream outputStream = new FileOutputStream("C://test//test1_down2.txt");
        //4:文件的拷贝      
        IOUtils.copy(inputStream, outputStream);
        //5:关闭流      
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }

    //判断某个路径下面的内容是文件或者文件夹  
    @Test
    public void isFileOrDir() throws URISyntaxException, IOException, InterruptedException {
        //1、获取FileSystem      
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration(), "root");
        //2、找到根目录/下面所有的文件或文件夹      
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/aaa/bbb/ccc/"));
        //3、判断
        for (FileStatus status : listStatus) {
            if (status.isDirectory()) {
                System.out.println(status.getPath().getName() + " 是文件夹");
            } else {
                System.out.println(status.getPath().getName() + " 是文件");
            }
        }
    }

    //重命名，并且具有剪切的功能。文件的移动和重命名。
    @Test
    public void moveRename() throws URISyntaxException, IOException, InterruptedException {
        //1:获取FileSystem（分布式文件系统）      
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration(),"root");
        //2、移动重命名操作      
        boolean b = fileSystem.rename(new Path("/aaa/bbb/ccc/hello.txt"), new Path("/aaa/bbb/ccc/ddd/hello.txt"));
        System.out.println(b);
        //3、关闭FileSystem      
        fileSystem.close();
    }

    //文件的追加
    @Test
    public void appendFile() throws URISyntaxException, IOException, InterruptedException {
        //1:获取FileSystem（分布式文件系统）      
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration(),"root");
        //2、追加操作
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream("C:\\test\\test.txt"));
        FSDataOutputStream outputStream = fileSystem.append(new Path("/aaa/bbb/ccc/hello1.txt"));
        IOUtils.copy(inputStream,outputStream);
        //3、关闭FileSystem      
        IOUtils.closeQuietly();
        fileSystem.close();
    }

    //删除文件或目录  
    @Test
    public void deleteFileOrDir() throws URISyntaxException, IOException, InterruptedException {
        //1:获取FileSystem（分布式文件系统）      
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration(),"root");
        //2、删除操作      
        // boolean b = fileSystem.delete(new Path("/test_big.txt"));      
        boolean b = fileSystem.delete(new Path("/aaa/bbb/ccc/test.txt"));
        System.out.println(b);
        //3、关闭FileSystem      
        fileSystem.close();
    }

    @Test
    // 小文件的合并上传
    public void mergeFile() throws URISyntaxException, IOException, InterruptedException {
        //1:获取FileSystem（分布式文件系统）      
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop10:8020"), new Configuration(),"root");
        //2:获取hdfs大文件的输出流      
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test_big.txt"));
        //3:获取一个本地文件系统      
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //4:获取本地文件夹下所有文件的详情      
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("C://test2"));
        //5:遍历每个文件，获取每个文件的输入流      
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());
            // 6:将小文件的数据复制到大文件          
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        //7:关闭流      
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }
}
