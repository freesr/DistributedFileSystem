package org.example;

import java.util.*;

public class ConsistentHashing {
    private final SortedMap<Integer, String> circle = new TreeMap<>();
    private final int numberOfReplicas;

    public ConsistentHashing(int numberOfReplicas, List<String> nodes) {
        this.numberOfReplicas = numberOfReplicas;
        for (String node : nodes) {
            add(node);
        }
    }

    private void add(String node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.put(hash(node.toString() + i), node);
        }
    }

    public void removeServer(String serverId) {
        remove(serverId);
        // Again, you may need to redistribute keys (client associations) here
    }


    public boolean isEmpty() {
        return circle.isEmpty();
    }

    public void addServer(String serverId) {
        add(serverId);
        // You may need to redistribute keys (client associations) here if necessary
    }


    public void remove(String node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.remove(hash(node.toString() + i));
        }
    }

    public String get(Object key) {
        if (circle.isEmpty()) {
            return null;
        }
        int hash = hash(key.toString());
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, String> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    private int hash(String key) {
        int hashCode = key.hashCode();
        int hashRingSize = 8; // The size of your hash ring
        return Math.abs(hashCode) % hashRingSize;
    }
}
