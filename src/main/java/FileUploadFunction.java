package com.example;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import com.azure.data.tables.*;
import com.azure.data.tables.models.*;
import com.azure.messaging.servicebus.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileUploadFunction {
    // Configuration settings
    private static final String STORAGE_CONNECTION_STRING = System.getenv("STORAGE_CONNECTION_STRING");
    private static final String SERVICE_BUS_CONNECTION_STRING = System.getenv("SERVICE_BUS_CONNECTION_STRING");
    private static final String SERVICE_BUS_QUEUE_NAME = System.getenv("SERVICE_BUS_QUEUE_NAME");
    private static final String TABLE_NAME = "FileMetadata";

    @FunctionName("FileUploadHandler")
    public void run(
            @EventGridTrigger(name = "event") String eventJson,
            final ExecutionContext context) {
        context.getLogger().info("Event Grid trigger function executed.");

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

            // Prepare metadata
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("FileName", fileName);
            metadata.put("Url", url);
            metadata.put("StorageAccount", storageAccount);
            metadata.put("ContentType", contentType);
            metadata.put("FileSize", fileSize);
            metadata.put("BlobType", blobType);
            metadata.put("EventTime", eventTime);
            metadata.put("UploadTimestamp", OffsetDateTime.now(ZoneOffset.UTC).toString());
            metadata.put("ProcessingStatus", "Pending");
            metadata.put("ExpiryTimestamp", OffsetDateTime.now(ZoneOffset.UTC).plusDays(7).toString());

            // Convert metadata to JSON string
            String metadataJson = objectMapper.writeValueAsString(metadata);

            // Log file information into Azure Table Storage
            TableClient tableClient = new TableClientBuilder()
                    .connectionString(STORAGE_CONNECTION_STRING)
                    .tableName(TABLE_NAME)
                    .buildClient();

            // Create the table if it doesn't exist
            try {
                tableClient.createTable();
                context.getLogger().info("Table created: " + TABLE_NAME);
            } catch (TableServiceException e) {
                if (e.getResponse().getStatusCode() != 409) { // 409 Conflict indicates table already exists
                    throw e;
                }
            }

            // Create a new entity
            String partitionKey = "FileMetadata"; // You can choose a partitioning strategy
            String rowKey = UUID.randomUUID().toString();

            TableEntity entity = new TableEntity(partitionKey, rowKey)
                    .addProperty("Metadata", metadataJson)
                    .addProperty("ExpiryTimestamp", metadata.get("ExpiryTimestamp"));

            // Insert the entity
            tableClient.createEntity(entity);

            context.getLogger().info("File information logged into Azure Table Storage.");

            // Send a message to Service Bus
            ServiceBusClientBuilder serviceBusClientBuilder = new ServiceBusClientBuilder()
                    .connectionString(SERVICE_BUS_CONNECTION_STRING);

            ServiceBusSenderClient senderClient = serviceBusClientBuilder
                    .sender()
                    .queueName(SERVICE_BUS_QUEUE_NAME)
                    .buildClient();

            // Create a message with file information
            ServiceBusMessage message = new ServiceBusMessage(metadataJson);

            // Send the message
            senderClient.sendMessage(message);
            senderClient.close();

            context.getLogger().info("Message sent to Service Bus queue.");

        } catch (Exception e) {
            context.getLogger().severe("Error processing the event: " + e.getMessage());
        }
    }
}
