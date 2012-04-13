package com.hazelcast.hbase;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;

public class Main {
    public static void main(String[] args) {
        IMap<String, User> map = Hazelcast.getMap("map");
        System.out.println( map.get("user-mehmet") );
        User user = new User();
        user.setName("Enes Akar");
        user.setAge(29);
        user.setLocation("Istanbul");
        user.setDetails("software developer .....");
        map.put("u-5",user);
        User user2 = new User();
        user2.setName("Mehmet Dogan");
        user2.setAge(29);
        user2.setLocation("Istanbul");
        user2.setDetails("software developer .....");
        map.put("u-6",user2);
        System.out.println( map.get("u-5") );
        System.out.println( map.get("u-6") );
        map.remove("u-5");
        System.out.println( map.get("u-5") );
    }

}
