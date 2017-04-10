package cs455.hadoop.util;

import java.util.*;

public class MapUtil
{
    public static <K, V extends Comparable<? super V>> Map<K, V>
    sortByValue(Map<K, V> map)
    {
        List<Map.Entry<K, V>> list =
                new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(list, (o1, o2) -> (o1.getValue()).compareTo(o2.getValue()));

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list)
        {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
