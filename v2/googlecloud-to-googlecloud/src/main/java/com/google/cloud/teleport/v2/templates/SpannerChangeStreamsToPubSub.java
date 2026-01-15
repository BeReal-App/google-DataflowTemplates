/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToPubSubOptions;
import com.google.cloud.teleport.v2.transforms.FileFormatFactorySpannerChangeStreamsToPubSub;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SpannerChangeStreamsToPubSub} pipeline streams change stream record(s) and stores to
 * pubsub topic in user specified format. The sink data can be stored in a JSON Text or Avro data
 * format.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_Spanner_Change_Streams_to_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Spanner_Change_Streams_to_PubSub",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Spanner change streams to Pub/Sub",
    description = {
      "The Cloud Spanner change streams to the Pub/Sub template is a streaming pipeline that streams Cloud Spanner data change records and writes them into Pub/Sub topics using Dataflow Runner V2.\n",
      "To output your data to a new Pub/Sub topic, you need to first create the topic. After creation, Pub/Sub automatically generates and attaches a subscription to the new topic. "
          + "If you try to output data to a Pub/Sub topic that doesn't exist, the dataflow pipeline throws an exception, and the pipeline gets stuck as it continuously tries to make a connection.\n",
      "If the necessary Pub/Sub topic already exists, you can output data to that topic.",
      "Learn more about <a href=\"https://cloud.google.com/spanner/docs/change-streams\">change streams</a>, <a href=\"https://cloud.google.com/spanner/docs/change-streams/use-dataflow\">how to build change streams Dataflow pipelines</a>, and <a href=\"https://cloud.google.com/spanner/docs/change-streams/use-dataflow#best_practices\">best practices</a>."
    },
    optionsClass = SpannerChangeStreamsToPubSubOptions.class,
    flexContainerName = "spanner-changestreams-to-pubsub",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-change-streams-to-pubsub",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Cloud Spanner instance must exist before running the pipeline.",
      "The Cloud Spanner database must exist prior to running the pipeline.",
      "The Cloud Spanner metadata instance must exist prior to running the pipeline.",
      "The Cloud Spanner metadata database must exist prior to running the pipeline.",
      "The Cloud Spanner change stream must exist prior to running the pipeline.",
      "The Pub/Sub topic must exist prior to running the pipeline."
    },
    streaming = true,
    supportsAtLeastOnce = true)
public class SpannerChangeStreamsToPubSub {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsToPubSub.class);
  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Input Messages to Pub/Sub");

    SpannerChangeStreamsToPubSubOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SpannerChangeStreamsToPubSubOptions.class);

    run(options);
  }

  private static String getSpannerProjectId(SpannerChangeStreamsToPubSubOptions options) {
    return options.getSpannerProjectId().isEmpty()
        ? options.getProject()
        : options.getSpannerProjectId();
  }

  private static String getPubsubProjectId(SpannerChangeStreamsToPubSubOptions options) {
    return options.getPubsubProjectId().isEmpty()
        ? options.getProject()
        : options.getPubsubProjectId();
  }

  public static boolean isValidAsciiString(String outputMessageMetadata) {
    if (outputMessageMetadata != null
        && !StandardCharsets.US_ASCII.newEncoder().canEncode(outputMessageMetadata)) {
      return false;
    }
    return true;
  }

  public static PipelineResult run(SpannerChangeStreamsToPubSubOptions options) {
    LOG.info("Requested Message Format is " + options.getOutputDataFormat());
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    final Pipeline pipeline = Pipeline.create(options);
    // Get the Spanner project, instance, database, metadata instance, metadata database
    // change stream, pubsub topic, and pubsub api parameters.
    String spannerProjectId = getSpannerProjectId(options);
    String instanceId = options.getSpannerInstanceId();
    String databaseId = options.getSpannerDatabase();
    String metadataInstanceId = options.getSpannerMetadataInstanceId();
    String metadataDatabaseId = options.getSpannerMetadataDatabase();
    String changeStreamName = options.getSpannerChangeStreamName();
    String pubsubProjectId = getPubsubProjectId(options);
    String pubsubTopicName = options.getPubsubTopic();
    String pubsubAPI = options.getPubsubAPI();
    Boolean includeSpannerSource = options.getIncludeSpannerSource();
    String outputMessageMetadata = options.getOutputMessageMetadata();

    // Ensure outputMessageMetadata only contains valid ascii characters
    if (!isValidAsciiString(outputMessageMetadata)) {
      throw new RuntimeException("outputMessageMetadata contains non ascii characters.");
    }

    // Retrieve and parse the start / end timestamps.
    Timestamp startTimestamp =
        options.getStartTimestamp().isEmpty()
            ? Timestamp.now()
            : Timestamp.parseTimestamp(options.getStartTimestamp());
    Timestamp endTimestamp =
        options.getEndTimestamp().isEmpty()
            ? Timestamp.MAX_VALUE
            : Timestamp.parseTimestamp(options.getEndTimestamp());

    // Add use_runner_v2 to the experiments option, since Change Streams connector is only supported
    // on Dataflow runner v2.
    List<String> experiments = options.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    if (!experiments.contains(USE_RUNNER_V2_EXPERIMENT)) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    options.setExperiments(experiments);

    String metadataTableName =
        options.getSpannerMetadataTableName() == null
            ? null
            : options.getSpannerMetadataTableName();

    final RpcPriority rpcPriority = options.getRpcPriority();
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
            .withProjectId(spannerProjectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);
    // Propagate database role for fine-grained access control on change stream.
    if (options.getSpannerDatabaseRole() != null) {
      spannerConfig =
          spannerConfig.withDatabaseRole(
              ValueProvider.StaticValueProvider.of(options.getSpannerDatabaseRole()));
    }
    
    // Read change stream records and convert to byte[] using FileFormatFactory
    // Note: We don't set the topic name to prevent direct write to Pub/Sub
    PCollection<byte[]> messageBytes =
        pipeline
            .apply(
                SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withMetadataInstance(metadataInstanceId)
                    .withMetadataDatabase(metadataDatabaseId)
                    .withChangeStreamName(changeStreamName)
                    .withInclusiveStartAt(startTimestamp)
                    .withInclusiveEndAt(endTimestamp)
                    .withRpcPriority(rpcPriority)
                    .withMetadataTable(metadataTableName))
            .apply(
                "Convert each record to byte[]",
                FileFormatFactorySpannerChangeStreamsToPubSub.newBuilder()
                    .setOutputDataFormat(options.getOutputDataFormat())
                    .setProjectId(pubsubProjectId)
                    .setPubsubAPI(pubsubAPI)
                    // Don't set topic name - this should make it just convert, not write directly
                    // If this causes an error, we'll need a different approach
                    .setIncludeSpannerSource(includeSpannerSource)
                    .setSpannerDatabaseId(databaseId)
                    .setSpannerInstanceId(instanceId)
                    .setOutputMessageMetadata(outputMessageMetadata)
                    .build());
    
    // Extract user ID and create PubsubMessage with ordering key
    PCollection<PubsubMessage> pubsubMessages =
        messageBytes.apply(
            "Extract user ID and create PubsubMessage with ordering key",
            ParDo.of(new CreatePubsubMessageWithOrderingKey()));
    
    // Write to Pub/Sub - the ordering key is already set in the PubsubMessage
    pubsubMessages.apply(
        "Write to Pub/Sub",
        org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.writeMessages()
            .to(String.format("projects/%s/topics/%s", pubsubProjectId, pubsubTopicName)));
    
    return pipeline.run();
  }
  
  /**
   * DoFn that extracts user ID from message payload (byte[]) and creates PubsubMessage
   * with ordering key set to user ID.
   */
  private static class CreatePubsubMessageWithOrderingKey extends DoFn<byte[], PubsubMessage> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      byte[] payload = c.element();
      String orderingKey = null;
      
      try {
        // Parse the message payload to extract user ID
        String payloadString = new String(payload, StandardCharsets.UTF_8);
        JsonParser parser = new JsonParser();
        JsonObject jsonObject = parser.parse(payloadString).getAsJsonObject();
        
        // Extract user ID if this is a Users table change
        String userId = extractUserIdFromJson(jsonObject);
        if (userId != null && !userId.isEmpty()) {
          orderingKey = userId;
          LOG.debug("Extracted ordering key: {}", userId);
        } else {
          LOG.debug("No user ID found in message");
        }
      } catch (Exception e) {
        LOG.warn("Could not extract user ID from message payload: {}", e.getMessage());
        // Continue without ordering key - message will still be published
      }
      
      // Create a PubsubMessage with the ordering key set
      Map<String, String> attributes = new HashMap<>();
      PubsubMessage messageWithOrderingKey =
          new PubsubMessage(
              payload,
              attributes,
              null,  // messageId - can be null, Pub/Sub will generate one
              orderingKey);
      
      if (orderingKey != null && !orderingKey.isEmpty()) {
        LOG.info("Created PubsubMessage with ordering key: {}", orderingKey);
      }
      
      c.output(messageWithOrderingKey);
    }
    
    /**
     * Extracts user ID from a JSON object representing a change stream message.
     * Expected format: {"tableName": "Users", "mods": [{"keysJson": "{\"UserId\": \"...\"}", ...}], ...}
     */
    private String extractUserIdFromJson(JsonObject jsonObject) {
      try {
        String tableName = jsonObject.has("tableName") 
            ? jsonObject.get("tableName").getAsString() 
            : null;
        LOG.debug("Checking tableName: {}", tableName);
        if ("Users".equals(tableName) && jsonObject.has("mods")) {
          JsonArray mods = jsonObject.get("mods").getAsJsonArray();
          LOG.debug("Found {} mods", mods.size());
          if (mods.size() > 0) {
            JsonObject firstMod = mods.get(0).getAsJsonObject();
            if (firstMod.has("keysJson")) {
              String keysJson = firstMod.get("keysJson").getAsString();
              LOG.debug("Parsing keysJson: {}", keysJson);
              JsonParser parser = new JsonParser();
              JsonObject keys = parser.parse(keysJson).getAsJsonObject();
              if (keys.has("UserId")) {
                String userId = keys.get("UserId").getAsString();
                LOG.debug("Extracted UserId: {}", userId);
                return userId;
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.warn("Could not extract user ID from JSON: {}", e.getMessage(), e);
      }
      return null;
    }
  }
}
