package com.example;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import com.azure.data.tables.*;
import com.azure.data.tables.models.*;
import com.azure.core.util.Context;
import com.azure.messaging.servicebus.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class d1 {
    // Environment variables or configuration settings
    private static final String COSMOS_DB_CONNECTION_STRING = System.getenv("COSMOS_DB_CONNECTION_STRING");
    private static final String SERVICE_BUS_CONNECTION_STRING = System.getenv("SERVICE_BUS_CONNECTION_STRING");
    private static final String SERVICE_BUS_QUEUE_NAME = System.getenv("SERVICE_BUS_QUEUE_NAME");
    private static final String TABLE_NAME = "FileMetadata";

    @FunctionName("FileUploadHandler")
    public void run(
            @EventGridTrigger(name = "event") String eventJson,
            final ExecutionContext context) {
        context.getLogger().info("Java Event Grid trigger function executed.");

        try {
            // Parse the Event Grid event
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode eventNode = objectMapper.readTree(eventJson);

            // Extract event data
            JsonNode dataNode = eventNode.get("data");
            String url = dataNode.get("url").asText();
            String fileName = url.substring(url.lastIndexOf('/') + 1);
            String storageAccount = dataNode.get("storageAccount").asText();
            String contentType = dataNode.get("contentType").asText();
            long fileSize = dataNode.get("contentLength").asLong();
            String blobType = dataNode.get("blobType").asText();
            String eventTime = eventNode.get("eventTime").asText();

            // Log file information into Cosmos DB Table API
            TableClient tableClient = new TableClientBuilder()
                    .connectionString(COSMOS_DB_CONNECTION_STRING)
                    .tableName(TABLE_NAME)
                    .buildClient();

            // Create the table if it doesn't exist
            tableClient.createTable();

            // Create a new entity
            TableEntity entity = new TableEntity(UUID.randomUUID().toString(), fileName)
                    .addProperty("Url", url)
                    .addProperty("StorageAccount", storageAccount)
                    .addProperty("ContentType", contentType)
                    .addProperty("FileSize", fileSize)
                    .addProperty("BlobType", blobType)
                    .addProperty("EventTime", eventTime)
                    .addProperty("UploadTimestamp", OffsetDateTime.now().toString())
                    .addProperty("ProcessingStatus", "Pending");

            // Set TTL (in seconds)
            int ttlInSeconds = 7 * 24 * 60 * 60; // 7 days
            entity.addProperty("ttl", ttlInSeconds);

            // Insert the entity
            tableClient.createEntity(entity);

            context.getLogger().info("File information logged into Cosmos DB Table.");

            // Send a message to Service Bus
            ServiceBusClientBuilder serviceBusClientBuilder = new ServiceBusClientBuilder()
                    .connectionString(SERVICE_BUS_CONNECTION_STRING);

            ServiceBusSenderClient senderClient = serviceBusClientBuilder
                    .sender()
                    .queueName(SERVICE_BUS_QUEUE_NAME)
                    .buildClient();

            // Create a message with file information
            Map<String, Object> messageData = new HashMap<>();
            messageData.put("FileName", fileName);
            messageData.put("Url", url);
            messageData.put("FileSize", fileSize);
            messageData.put("ContentType", contentType);
            messageData.put("EventTime", eventTime);

            String messageBody = objectMapper.writeValueAsString(messageData);

            ServiceBusMessage message = new ServiceBusMessage(messageBody);

            // Send the message
            senderClient.sendMessage(message);
            senderClient.close();

            context.getLogger().info("Message sent to Service Bus queue.");

        } catch (Exception e) {
            context.getLogger().severe("Error processing the event: " + e.getMessage());
        }
    }
}
