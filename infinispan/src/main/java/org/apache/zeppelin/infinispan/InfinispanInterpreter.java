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
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;

/**
 * Infinispan (http://infinispan.org) interpreter for Zeppelin.
 */
public class InfinispanInterpreter extends Interpreter {

   private RemoteCache<?, ?> remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private SerializationContext serializationContext;
   private final Pattern entityPattern = Pattern.compile("\\s?FROM\\s+(\\S+)", Pattern.CASE_INSENSITIVE);

   public InfinispanInterpreter(Properties property) {
      super(property);
   }

   @Override
   public void open() {
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(11222);  //TODO(gustavo): expose as properties
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();

      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      serializationContext = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      Set<String> keys = metadataCache.keySet();
      for (String key : keys) {
         String proto = metadataCache.get(key);
         try {
            serializationContext.registerProtoFiles(FileDescriptorSource.fromString(key, proto));
         } catch (IOException e) {
            e.printStackTrace();
         }
      }

      serializationContext.getFileDescriptors().values().stream()
            .flatMap(fd -> fd.getMessageTypes().stream())
            .forEach(md -> serializationContext.registerMarshaller(new ZeppelinMessageMarshaller(md.getFullName())));
   }

   private String extractTarget(String sqlQuery) {
      Matcher matcher = entityPattern.matcher(sqlQuery);
      if (matcher.find()) {
         return matcher.group(1);
      } else {
         throw new InterpreterException("Could not extract target type from the query");
      }

   }

   private static String printFields(Query remoteQuery, SerializationContext serializationContext, String givenType) {
      StringBuilder output = new StringBuilder();

      StringJoiner stringJoiner = new StringJoiner("\t");
      BaseQuery baseQuery = (BaseQuery) remoteQuery;
      String[] projection = baseQuery.getProjection();
      boolean hasProjection = projection != null && projection.length > 0;
      if (hasProjection) {
         Arrays.stream(projection).forEach(stringJoiner::add);
      } else {
         Descriptor messageDescriptor = serializationContext.getMessageDescriptor(givenType);
         List<FieldDescriptor> fields = messageDescriptor.getFields();
         fields.forEach(fd -> stringJoiner.add(fd.getName()));
      }
      return output.append(stringJoiner.toString()).append("\n").toString();
   }

   private static String printValues(Query remoteQuery) {
      List<Object> list = remoteQuery.list();

      StringJoiner stringJoiner = new StringJoiner("\t");
      StringJoiner stringJoiner1 = new StringJoiner("\n");
      BaseQuery baseQuery = (BaseQuery) remoteQuery;
      String[] projection = baseQuery.getProjection();
      boolean hasProjection = projection != null && projection.length > 0;
      if (hasProjection) {
         list.stream().map(o -> (Object[]) o)
               .map(arr -> {
                  Arrays.stream(arr).forEach(o -> stringJoiner.add(o.toString()));
                  return stringJoiner.toString();
               }).forEach(stringJoiner1::add);
      } else {
         list.forEach(l -> stringJoiner1.add(l.toString()));
      }
      return stringJoiner1.toString();
   }


   @Override
   public void close() {
      remoteCacheManager.stop();
   }

   @Override
   public InterpreterResult interpret(String query, InterpreterContext context) {
      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);
      Query remoteQuery = queryFactory.from(String.class).build();
      BaseQuery baseQuery = (BaseQuery) remoteQuery;
      try {
         Field f = BaseQuery.class.getDeclaredField("jpaQuery");
         f.setAccessible(true);
         f.set(baseQuery, query);
      } catch (NoSuchFieldException | IllegalAccessException e) {
         e.printStackTrace();
      }

      String top = printFields(remoteQuery, serializationContext, extractTarget(query));
      String bottom = printValues(remoteQuery);

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE, top + bottom);
   }

   @Override
   public void cancel(InterpreterContext context) {
   }

   @Override
   public FormType getFormType() {
      return FormType.SIMPLE;
   }

   @Override
   public int getProgress(InterpreterContext context) {
      return 0;
   }
}
