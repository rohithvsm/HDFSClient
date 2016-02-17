package com.sankalpa.bigdata.hdfs.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSClient {
    private File coreSiteConfPath;
    private File hdfsSiteConfPath;

    private Path coreSitePath;
    private Path hdfsSitePath;

    private Configuration conf;
    private FileSystem fileSystem;

    // The config directory is passed by the user's main program
    public HDFSClient(String siteConfDir) throws IOException {
        if (validatePath(siteConfDir) == 1) {
            System.out.println("Error validating path. Exiting...");
            System.exit(1);
        }

        coreSitePath = new Path(coreSiteConfPath.getPath()); // getPath() returns a String
        hdfsSitePath = new Path(hdfsSiteConfPath.getPath());

        conf = new Configuration();
        // Conf object will read the HDFS configuration parameters from these
        // XML files.
        conf.addResource(coreSitePath);
        conf.addResource(hdfsSitePath);

        try {
          fileSystem = FileSystem.get(conf);

        } catch(IOException e) { // failed or interrupted I/O operations
          System.out.println("An exception has occurred during HDFSClient init: I/O exception: " + e.getMessage());
          System.exit(1);
        }
    }

    private int validatePath(String siteConfDir) {
        File siteConfDirPath = new File(siteConfDir);

        if (siteConfDirPath.exists() == false) {
            System.out.println("ERROR: The argument " + siteConfDirPath.getPath() + " does not exist.");
            return 1;
        }
        if (siteConfDirPath.isDirectory() == false) {
            System.out.println("ERROR: The argument " + siteConfDirPath.getPath() + " must be a directory.");
            return 1;
        }
        if (siteConfDirPath.canRead() == false) {
            System.out.println("ERROR: The argument " + siteConfDirPath.getPath() + " is not readable by me.");
            return 1;
        }

        coreSiteConfPath = new File(siteConfDirPath, "core-site.xml");
        if (coreSiteConfPath.exists() == false) {
            System.out.println("ERROR: The argument " + coreSiteConfPath.getPath() + " does not exist.");
            return 1;
        }
        if (coreSiteConfPath.isFile() == false) {
            System.out.println("ERROR: The argument " + coreSiteConfPath.getPath() + " must be a file.");
            return 1;
        }
        if (coreSiteConfPath.canRead() == false) {
            System.out.println("ERROR: The argument " + coreSiteConfPath.getPath() + " is not readable by me.");
            return 1;
        }

        hdfsSiteConfPath = new File(siteConfDirPath, "hdfs-site.xml");
        if (hdfsSiteConfPath.exists() == false) {
            System.out.println("ERROR: The argument " + hdfsSiteConfPath.getPath() + " does not exist.");
            return 1;
        }
        if (hdfsSiteConfPath.isFile() == false) {
            System.out.println("ERROR: The argument " + hdfsSiteConfPath.getPath() + " must be a file.");
            return 1;
        }
        if (hdfsSiteConfPath.canRead() == false) {
            System.out.println("ERROR: The argument " + hdfsSiteConfPath.getPath() + " is not readable by me.");
            return 1;
        }
        return 0;
    }

    public void addFile(String source, String dest) throws IOException {
        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1,
            source.length());

        // Create the destination path including the filename
        if (dest.charAt(dest.length() - 1) != '/') {
            dest = dest + "/" + filename;
        } else {
            dest = dest + filename;
        }

        System.out.println("Adding file to " + dest);

        // Check if the file already exists
        Path path = new Path(dest);
        if (fileSystem.exists(path)) {
            System.out.println("File " + dest + " already exists");
            return;
        }

        // Create a new file and write data to it
        FSDataOutputStream out = fileSystem.create(path);
        InputStream in = new BufferedInputStream(new FileInputStream(
            new File(source)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        // Close all the file descriptors
        in.close();
        out.close();
        fileSystem.close();
    }

    public void readFile(String file) throws IOException {
        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exist");
            return;
        }

        FSDataInputStream in = fileSystem.open(path);

        String filename = file.substring(file.lastIndexOf('/') + 1,
            file.length());

        OutputStream out = new BufferedOutputStream(new FileOutputStream(
            new File(filename)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
        fileSystem.close();
    }

    public void deleteFile(String file) throws IOException {
        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exist");
            return;
        }

        fileSystem.delete(new Path(file), true);

        fileSystem.close();
    }

    public void mkdir(String dir) throws IOException {
        Path path = new Path(dir);
        if (fileSystem.exists(path)) {
            System.out.println("Dir " + dir + " already exists");
            return;
        }

        fileSystem.mkdirs(path);

        fileSystem.close();
    }

    private static void usage() {
        System.out.println("Usage: HDFSClient add/read/delete/mkdir" +
            " [<local_path> <hdfs_path>]");
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            usage();
            System.exit(1);
        }

        if (args[0].equals("--help") || args[0].equals("-h")) {
            usage();
            System.exit(0);
        }

        System.out.println("HDFSClient: Starting...");
        HDFSClient client = new HDFSClient("/Users/rohithvsm/workspace/big_data/hadoop/hadoop-dist/target/hadoop-2.6.0/etc/hadoop");

        if (args[0].equals("add")) {
            if (args.length < 3) {
                System.out.println("Usage: HDFSClient add <local_path> " + 
                "<hdfs_path>");
                System.exit(1);
            }

            client.addFile(args[1], args[2]);
        } else if (args[0].equals("read")) {
            if (args.length < 2) {
                System.out.println("Usage: HDFSClient read <hdfs_path>");
                System.exit(1);
            }

            client.readFile(args[1]);
        } else if (args[0].equals("delete")) {
            if (args.length < 2) {
                System.out.println("Usage: HDFSClient delete <hdfs_path>");
                System.exit(1);
            }

            client.deleteFile(args[1]);
        } else if (args[0].equals("mkdir")) {
            if (args.length < 2) {
                System.out.println("Usage: HDFSClient mkdir <hdfs_path>");
                System.exit(1);
            }

            client.mkdir(args[1]);
        } else {   
            usage();
            System.exit(1);
        }

        System.out.println("HDFSClient: Done!");
    }
}
