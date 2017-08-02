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

import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import lombok.val;
import org.springframework.cloud.stream.app.grpc.message.Generic;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

/**
 * @author David Turanski
 **/
public class ToGenericConverter implements Converter<Object, Generic> {

	@Override
	public Generic convert(Object obj) {

		Assert.notNull(obj,"source object cannot be null");

		Generic val;

		if (obj instanceof Boolean) {

			val = Generic.newBuilder().setBool((boolean)obj).build();
		}

		else if (obj instanceof byte[]) {
			val =  Generic.newBuilder().setBytes(ByteString.copyFrom((byte[]) obj)).build();
		}

		else if (obj instanceof Double) {
			val =  Generic.newBuilder().setDouble((double) obj).build();
		}

		else if (obj instanceof Float) {
			val =  Generic.newBuilder().setFloat((float) obj).build();
		}

		else if (obj instanceof Integer) {
			val =  Generic.newBuilder().setInt((int) obj).build();
		}

		else if (obj instanceof Long) {
			val =  Generic.newBuilder().setLong((long) obj).build();
		}

		else if (obj instanceof Short) {
			val =  Generic.newBuilder().setInt((int) obj).build();
		}

		else if (obj instanceof String) {
			val =  Generic.newBuilder().setString((String) obj).build();
		}

		else {
			val =  Generic.newBuilder().setString(obj.toString()).build();

		}

		return val;
	}
}
