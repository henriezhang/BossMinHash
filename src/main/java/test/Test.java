package test;
//import redis.clients.jedis.Jedis;


public class Test {
    public static void main(String[] args) {
        System.out.println("HelloWorld!");
        String str = "868428010455743,00a767888a5311e3b068abcd018be70a,,00:0c:e7:00:02:ca,10:0.22057766931674672;3:0.05182082324903415;5:0.06989743575210065;25:0.12398582955279941;31:0.08273697548218358;1:0.04643789384639689;2:0.17873991072608444;43:0.23164835604374348;60:0.014399858423359287,16384:0.09852205810796455;1:0.19415093444902343;4194304:0.20370260219079403;8388608:0.09578458985107569;16:0.11085516009536675;4:0.10838513160215633;3:0.10708366938579249,262144:0.12080594725911835;1048576:0.16975977928493874;64:0.14251921560603606;4096:0.11965529037795644;1024:0.2670718884605366;2097152:0.120480401361889,16384:0.11981385199161047;30:0.20868485418853017;4:0.20565643392383928,4:0.05107832091142952;5:0.08116004840453221;22:0.20095831707105238;25:0.051019645276156315;27:0.0824061824395014;9:0.07634316003592001;1:0.2775109693223196;3:0.07792509482852217;2:0.10163943888180195,";
        System.out.println("--------------");
        String fields[] = str.split(",");
        System.out.println("length:"+fields.length);
        for(String item: fields) {
            System.out.println(item);
        }
        System.out.println("--------------");

/*        String hosts ="10.187.141.112:9080,10.187.5.149:9080,10.187.5.161:9080,10.204.16.214:9080";
        String hs[] = hosts.split(",");
        java.util.Random r = new java.util.Random();
        int idx = Math.abs(r.nextInt()) % hs.length;
        System.out.println(hs.length);
        System.out.println(idx);
        String item[] = hs[idx].split(":");
        
        System.out.println("HelloWorld!"+item[0]+item[1]);
*/
        return;
/*
        try{
            String name = "tcbdredis.redis.com";         
            Runtime run = Runtime.getRuntime();
            Process p = run.exec("/usr/bin/zkname "+name); 
            BufferedInputStream in = new BufferedInputStream(p.getInputStream());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String s;
            while ((s = br.readLine()) != null) {
                String[] fields = s.split("\\t", 10);
                if(fields.length == 2) {
                    String ip = fields[0];
                    Integer port = Integer.parseInt(fields[1]);
                    Jedis cfRedis = new Jedis(ip, port);
                    cfRedis.set("405676994", "hello henriezhang");  
                    long expTime = new Date().getTime()/1000;
                    System.out.println(expTime);
                    cfRedis.expireAt("405676994", expTime+24*60*60);
                    System.out.println(cfRedis.get("405676994"));
                }
            }
        }
        catch(Exception e)  {
            System.out.println(e.toString());
        }
    */
    }
}
