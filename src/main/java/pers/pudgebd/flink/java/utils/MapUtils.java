package pers.pudgebd.flink.java.utils;

import java.util.*;
import java.util.Map.Entry;

/**
 * @version 1.4.1
 */
public class MapUtils {

	public static <K, V> void fillKeySetMap(Map<K, Set<V>> keySetMap, K key, V val) {
		Set<V> valSet = keySetMap.get(key);
		if (valSet == null) {
			valSet = new HashSet<V>();
			valSet.add(val);
			keySetMap.put(key, valSet);
		} else {
			valSet.add(val);
		}
	}

	public static <K, V> void fillKeyHashSetMap(Map<K, HashSet<V>> keySetMap, K key, V val) {
		HashSet<V> valSet = keySetMap.get(key);
		if (valSet == null) {
			valSet = new HashSet<V>();
			valSet.add(val);
			keySetMap.put(key, valSet);
		} else {
			valSet.add(val);
		}
	}

	public static <K1, K2, V> void fillKey1Key2ValMap(Map<K1, Map<K2, V>> key1Key2ValMap, K1 outKey, K2 innerKey,
			V strVal, boolean isAddLastVal) {
		Map<K2, V> key2ValMap = key1Key2ValMap.get(outKey);
		if (key2ValMap == null) {
			key2ValMap = new HashMap<K2, V>();
			key2ValMap.put(innerKey, strVal);
			key1Key2ValMap.put(outKey, key2ValMap);
		} else {
			if (isAddLastVal) {
				V existsVal = key2ValMap.get(innerKey);
				if (existsVal == null) {
					key2ValMap.put(innerKey, strVal);
				} else {
					Long existsValLong = Long.parseLong(existsVal.toString());
					Long finalValLong = Long.parseLong(strVal.toString()) + existsValLong;
					key2ValMap.put(innerKey, (V) finalValLong);
				}
			} else {
				key2ValMap.put(innerKey, strVal);
			}
		}
	}

	/**
	 * there is no meaning to fill strStrMap here
	 */
	public static <K> void fillKeyLongMapAddUpVal(Map<K, Long> keyLongMap, K key, long val) {
		Long existsVal = keyLongMap.get(key);
		if (existsVal != null) {
			val = existsVal + val;
		}
		keyLongMap.put(key, val);
	}

	public static void fillStrStrMapAddUpVal(Map<String, String> strStrMap, String key, String val) {
		String existsVal = strStrMap.get(key);
		if (existsVal != null) {
			val = String.valueOf(Long.parseLong(existsVal) + Long.parseLong(val));
		}
		strStrMap.put(key, val);
	}

	public static <K1, K2, V> void fillKey1Key2ValSetMap(Map<K1, Map<K2, Set<V>>> k1k2ValSetMap, K1 outKey, K2 innerKey,
			V val) {
		Map<K2, Set<V>> k2ValSetMap = k1k2ValSetMap.get(outKey);

		if (k2ValSetMap == null) {
			k2ValSetMap = new HashMap<K2, Set<V>>();
			Set<V> innerSet = new HashSet<V>();
			innerSet.add(val);
			k2ValSetMap.put(innerKey, innerSet);
			k1k2ValSetMap.put(outKey, k2ValSetMap);

		} else {
			Set<V> innerSet = k2ValSetMap.get(innerKey);
			if (innerSet == null) {
				innerSet = new HashSet<V>();
				innerSet.add(val);
				k2ValSetMap.put(innerKey, innerSet);
			} else {
				innerSet.add(val);
			}
		}
	}

	public static <K1, K2, V> void fillKey1Key2ValListMap(Map<K1, Map<K2, List<V>>> k1k2ValListMap, K1 outKey,
			K2 innerKey, V val) {
		Map<K2, List<V>> k2ValListMap = k1k2ValListMap.get(outKey);

		if (k2ValListMap == null) {
			k2ValListMap = new HashMap<K2, List<V>>();
			List<V> innerList = new ArrayList<V>();
			innerList.add(val);
			k2ValListMap.put(innerKey, innerList);
			k1k2ValListMap.put(outKey, k2ValListMap);

		} else {
			List<V> innerList = k2ValListMap.get(innerKey);
			if (innerList == null) {
				innerList = new ArrayList<V>();
				innerList.add(val);
				k2ValListMap.put(innerKey, innerList);
			} else {
				innerList.add(val);
			}
		}
	}

	public static void fillStrStrStrStrMap(Map<String, Map<String, Map<String, String>>> k1k2k3ValMap, String key1,
			String key2, String key3, String lastVal, boolean isAddLastVal) {

		Map<String, Map<String, String>> k2k3valMap = k1k2k3ValMap.get(key1);

		if (k2k3valMap == null) {
			k2k3valMap = new HashMap<String, Map<String, String>>();
			Map<String, String> strStrMap = new HashMap<String, String>();
			strStrMap.put(key3, lastVal);
			k2k3valMap.put(key2, strStrMap);
			k1k2k3ValMap.put(key1, k2k3valMap);

		} else {

			Map<String, String> strStrMap = k2k3valMap.get(key2);
			if (strStrMap == null) {
				strStrMap = new HashMap<String, String>();
				strStrMap.put(key3, lastVal);
				k2k3valMap.put(key2, strStrMap);
			} else {
				if (isAddLastVal) {
					String existValStr = strStrMap.get(key3);
					int existVal = 0;
					if (existValStr != null) {
						existVal = Integer.parseInt(existValStr);
					}
					existVal += Integer.parseInt(lastVal);
					strStrMap.put(key3, String.valueOf(existVal));
				} else {
					strStrMap.put(key3, lastVal);
				}
			}
		}
	}

	public static Map<String, String> mergeTwoStrStrMapAddVal(Map<String, String> alive, Map<String, String> dying) {
		for (Entry<String, String> entry : dying.entrySet()) {
			String dyingKey = entry.getKey();
			String dyingVal = entry.getValue();
			String aliveVal = alive.get(dyingKey);
			if (aliveVal == null) {
				aliveVal = String.valueOf(Integer.parseInt(aliveVal) + Integer.parseInt(dyingVal));
				alive.put(dyingKey, aliveVal);
			} else {
				alive.put(dyingKey, dyingVal);
			}
		}
		return alive;
	}

	public static <K, T> void fillKeyListMap(Map<K, List<T>> originalMap, K key, T listEle) {
		List<T> tempList = originalMap.get(key);
		if (tempList == null) {
			tempList = new ArrayList<T>();
			tempList.add(listEle);
			originalMap.put(key, tempList);
		} else {
			tempList.add(listEle);
		}
	}

	public static <K, T> void fillKeyListMap(Map<K, List<T>> originalMap, K key, List<T> listEle) {
		List<T> tempList = originalMap.get(key);
		if (tempList == null) {
			tempList = new ArrayList<T>();
			tempList.addAll(listEle);
			originalMap.put(key, tempList);
		} else {
			tempList.addAll(listEle);
		}
	}

	public static <K, T> void fillKeyArrListMap(Map<K, ArrayList<T>> originalMap, K key, T listEle) {
		ArrayList<T> tempList = originalMap.get(key);
		if (tempList == null) {
			tempList = new ArrayList<T>();
			tempList.add(listEle);
			originalMap.put(key, tempList);
		} else {
			tempList.add(listEle);
		}
	}

	/**
	 * @param strArr len != 0 && len % 2 == 0
	 * @return
	 */
	public static Map<String, String> getMapFromStrArr(String... strArr) {
		Map<String, String> resultMap = new HashMap<String, String>();
		int len = strArr.length;
		if (len != 0 && len % 2 == 0) {
			for (int i = 0; i < len; i += 2) {
				resultMap.put(strArr[i], strArr[i + 1]);
			}
		}
		return resultMap;
	}

}
