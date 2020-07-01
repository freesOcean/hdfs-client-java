package com.zd.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import java.net.URI;

public class HDFSClientAPI {

    private String url = "hdfs://192.168.43.11:9000";
    private String user = "xiaomao";
    /**
     * 获取一个只有基本配置FileSystem对象
     * @return
     */
    public FileSystem createCommonFS() {
        try {
            Configuration conf= new Configuration();
            //如果对接的文件系统
            FileSystem fs = FileSystem.get(new URI(url), conf, user);
            return fs;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 创建目录
     * @throws Exception
     */
    @Test
    public void mkdirs() throws Exception{
        FileSystem fs = createCommonFS();
        // 2 创建目录
        fs.mkdirs(new Path("/zx/7117"));
        // 3 关闭资源
        fs.close();
    }


    /**
     * 删除文件或文件夹
     * @throws Exception
     */
    @Test
    public void delete() throws Exception{
        FileSystem fs = createCommonFS();
        //如果删除是目录recursive必须设置为true,表示递归删除。如果是文件,true和false都可以。
        fs.delete(new Path("/zx/7117"),true);
        fs.close();
    }


    /**
     * 上传文件
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception{
        FileSystem fs = createCommonFS();
        // 如果参数为 /zx/file/a.txt 表示重命名为a.txt
        fs.copyFromLocalFile(new Path("f:/hdfs.txt"),new Path("/zx/file/"));
    }


    /**
     * 下载文件
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception{
        FileSystem fs = createCommonFS();
        // 表示将file目下的文件拷贝到当前f盘，如果指定文件名，则是拷贝指定文件到本地
        fs.copyToLocalFile(new Path("/zx/file/"),new Path("f:/"));
    }

    /**
     * 修改文件夹或文件名
     * @throws Exception
     */
    @Test
    public void rename() throws Exception{
        FileSystem fs = createCommonFS();
        fs.rename(new Path("/zx/file"),new Path("/zx/files"));
    }

    /**
     * 查看文件详情
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception{
        FileSystem fs = createCommonFS();
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.isFile()?"文件":"文件夹");//判断文件类型
            System.out.println(fileStatus.getPath().getName());// 文件名称
            System.out.println(fileStatus.getPermission());// 文件权限
            System.out.println(fileStatus.getLen());// 文件长度

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for(BlockLocation blockLocation:blockLocations){
                String[] hosts = blockLocation.getHosts();
                for(String host:hosts){
                    System.out.println(host);
                }
            }

        }

        fs.close();
    }

    /**
     * 判断是文件夹还是文件
     * @throws Exception
     */
    @Test
    public void isFile() throws Exception{
        FileSystem fs = createCommonFS();
        // 2 判断操作
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            if (fileStatus.isFile()) {
                // 文件
                System.out.println("f:"+fileStatus.getPath().getName());
            }else{
                // 文件夹
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }




    /**
     * 检测环境变量
     */
    @Test
    public void testEnv(){
        System.out.println(System.getenv("HADOOP_HOME"));
    }
}
