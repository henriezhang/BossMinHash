package test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;

public class ZkName implements Serializable {
    String ip;
    Integer port;
    boolean stat = false;

    public ZkName(String name) {
        try {
            Runtime run = Runtime.getRuntime();
            Process p = run.exec("/usr/bin/zkname " + name);
            BufferedInputStream in = new BufferedInputStream(p.getInputStream());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String s;
            while ((s = br.readLine()) != null) {
                System.out.println(s);
                String[] fields = s.split("\\t");
                if (fields.length == 2) {
                    ip = fields[0];
                    port = Integer.parseInt(fields[1]);
                    System.out.println(ip);
                    System.out.println(port);
                    stat = true;
                }
            }
        } catch (Exception e) {
            System.out.println(e.toString());
            ip = "10.204.16.214";
            port = 9080;
            stat = true;
        }
    }

    public String getIp() {
        return ip;
    }

    public Integer getPort() {
        return port;
    }

    public boolean getStat() {
        return stat;
    }
}


