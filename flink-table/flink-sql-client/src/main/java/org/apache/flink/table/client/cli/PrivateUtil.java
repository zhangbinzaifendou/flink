package org.apache.flink.table.client.cli;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PrivateUtil {
	public static Map<String,Method> methodMap = new ConcurrentHashMap<>();

	public static Method getMethod(Class clazz, String methodName, final Class[] paramsType) throws Exception {
		Method method = null;
		try {
			if (methodMap.get(methodName) == null) {
				method = clazz.getSuperclass().getDeclaredMethod(methodName, paramsType);
				method.setAccessible(true);
				methodMap.put(methodName, method);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return methodMap.get(methodName);
	}

	public static Object invoke(final Object obj, final String methodName) {
		return invoke(obj, methodName, new Class[]{}, new Object[]{});
	}

	public static Object invoke(final Object obj, final String methodName,
								final Class[] classes) {
		return invoke(obj, methodName, classes, new Object[]{});
	}

	public static Object invoke(final Object obj, final String methodName,
								final Class[] paramsType, final Object[] args) {
		try {
			Method method = getMethod(obj.getClass(), methodName, paramsType);
			return method.invoke(obj, args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static Field getParentPrivateVar(Class clazz, String fieldName) throws NoSuchFieldException {
		Field declaredField = clazz.getDeclaredField(fieldName);
		declaredField.setAccessible(true);
		return declaredField;
	}

}  
