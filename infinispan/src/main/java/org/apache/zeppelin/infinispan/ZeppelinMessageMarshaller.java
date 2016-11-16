/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.infinispan;

import java.io.IOException;
import java.util.List;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;

/**
 * A message marshaller that operates on String
 */
public class ZeppelinMessageMarshaller implements MessageMarshaller<String> {

   private final String givenType;

   public ZeppelinMessageMarshaller(String givenType) {
      this.givenType = givenType;
   }


   @Override
   public Class<? extends String> getJavaClass() {
      return String.class;
   }

   @Override
   public String getTypeName() {
      return givenType;
   }


   @Override
   public String readFrom(ProtoStreamReader reader) throws IOException {

      StringBuffer output = new StringBuffer();

      Descriptor messageDescriptor = reader.getSerializationContext().getMessageDescriptor(givenType);
      List<FieldDescriptor> fields = messageDescriptor.getFields();


      fields.forEach(fd -> {
         JavaType javaType = fd.getJavaType();
         if (javaType == JavaType.INT) {
            try {
               Integer value = reader.readInt(fd.getName());
               output.append(value);
               output.append("\t");
            } catch (IOException e) {
               e.printStackTrace();
            }
         }
         if (javaType == JavaType.STRING) {
            try {
               String value = reader.readString(fd.getName());
               output.append(value);
               output.append("\t");
            } catch (IOException e) {
               e.printStackTrace();
            }
         }
      });

      return output.toString();
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, String s) throws IOException {

   }

}
