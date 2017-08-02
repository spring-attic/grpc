/*
 * Copyright 2017 the original author or authors.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.springframework.cloud.stream.app.grpc.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.app.grpc.message.Generic;
import org.springframework.messaging.MessageHeaders;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author David Turanski
 **/
public class ProtobufMessageHeaders implements Map<String, Object> {
	private final Map<String, Object> headers;
	private final static Log log = LogFactory.getLog(ProtobufMessageHeaders.class);
	private final ToGenericConverter toGenericConverter = new ToGenericConverter();
	private final FromGenericConverter fromGenericConverter = new FromGenericConverter();

	public ProtobufMessageHeaders(MessageHeaders messageHeaders) {

		headers = new LinkedHashMap<>();
		for (Map.Entry<String, Object> header : messageHeaders.entrySet()) {
			headers.put(header.getKey(), toGenericConverter.convert(header.getValue()));
		}
	}

	public ProtobufMessageHeaders(Map<String,Generic> messageHeaders) {
		headers = new LinkedHashMap<>();
		headers.putAll(messageHeaders);
	}


	public boolean containsKey(Object key) {
		return this.headers.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return this.headers.containsValue(value);
	}

	public Set<Entry<String, Object>> entrySet() {
		return Collections.unmodifiableMap(this.headers).entrySet();
	}

	public Object get(Object key) {
		return fromGenericConverter.convert((Generic) headers.get(key));
	}

	public Map<String, Generic> asMap() {
		Map<String, Generic> genericMap = new LinkedHashMap<>();
		for (Map.Entry<String, Object> header : headers.entrySet()) {
			genericMap.put(header.getKey(), (Generic) header.getValue());
		}
		return genericMap;
	}

	public boolean isEmpty() {
		return this.headers.isEmpty();
	}

	public Set<String> keySet() {
		return Collections.unmodifiableSet(this.headers.keySet());
	}

	public int size() {
		return this.headers.size();
	}

	public Collection<Object> values() {
		return Collections.unmodifiableCollection(this.headers.values());
	}

	public Object put(String key, Object value) {
		throw new UnsupportedOperationException("MessageHeaders is immutable");
	}

	public void putAll(Map<? extends String, ? extends Object> map) {
		throw new UnsupportedOperationException("MessageHeaders is immutable");
	}

	public Object remove(Object key) {
		throw new UnsupportedOperationException("MessageHeaders is immutable");
	}

	public void clear() {
		throw new UnsupportedOperationException("MessageHeaders is immutable");
	}
}
