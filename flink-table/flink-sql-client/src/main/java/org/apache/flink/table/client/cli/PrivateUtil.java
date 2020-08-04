/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

	public static Object invokeParentMethod(final Object obj, final String methodName) {
		return invokeParentMethod(obj, methodName, new Class[]{}, new Object[]{});
	}

	public static Object invokeParentMethod(final Object obj, final String methodName,
											final Class[] classes) {
		return invokeParentMethod(obj, methodName, classes, new Object[]{});
	}

	public static Object invokeParentMethod(final Object obj, final String methodName,
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
