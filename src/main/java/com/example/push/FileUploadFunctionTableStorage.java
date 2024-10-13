package com.example.push;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableClientBuilder;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.EventGridTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class FileUploadFunctionTableStorage {
    private static final String STORAGE_CONNECTION_STRING = System.getenv("STORAGE_CONNECTION_STRING");
    private static final String SERVICE_BUS_CONNECTION_STRING = System.getenv("SERVICE_BUS_CONNECTION_STRING");
    private static final String SERVICE_BUS_QUEUE_NAME = System.getenv("SERVICE_BUS_QUEUE_NAME");
    private static final String TABLE_NAME = "FileMetadata";
    private static final String EXPIRY_TIMESTAMP = OffsetDateTime.now(ZoneOffset.UTC).plusDays(7).toString();

    @FunctionName("FileUploadHandler")
    public void run(
            @EventGridTrigger(name = "event") String eventJson,
            final ExecutionContext context) {
        context.getLogger().info("Event Grid trigger function executed.");

        try {
            String metadataJson = parseAndConvertEventToJson(eventJson);

            logDataToTableStorage(metadataJson, context);

            sendMessageToServiceBus(metadataJson, context);

        } catch (Exception e) {
            context.getLogger().severe("Error processing the event: " + e.getMessage());
        }
    }


    public String parseAndConvertEventToJson(String eventJson) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            JsonNode eventNode = objectMapper.readTree(eventJson);
            JsonNode dataNode = eventNode.get("data");

            if (dataNode == null) {
                throw new IllegalArgumentException("Event data is missing");
            }

            String url = getJsonTextValue(dataNode, "url");
            String fileName = extractFileNameFromUrl(url);
            String storageAccount = getJsonTextValue(dataNode, "storageAccount");
            String contentType = getJsonTextValue(dataNode, "contentType");
            long fileSize = getJsonLongValue(dataNode, "contentLength");
            String blobType = getJsonTextValue(dataNode, "blobType");
            String eventTime = getJsonTextValue(eventNode, "eventTime");

            Map<String, Object> metadata = prepareMetadata(fileName, url, storageAccount, contentType, fileSize, blobType, eventTime);

            return objectMapper.writeValueAsString(metadata);

        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Invalid event structure: " + e.getMessage(), e);

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse and convert event to JSON: " + e.getMessage(), e);
        }
    }

    private String getJsonTextValue(JsonNode node, String fieldName) {
        JsonNode valueNode = node.get(fieldName);
        if (valueNode == null || valueNode.isNull()) {
            throw new IllegalArgumentException("Missing or null field: " + fieldName);
        }
        return valueNode.asText();
    }

    private long getJsonLongValue(JsonNode node, String fieldName) {
        JsonNode valueNode = node.get(fieldName);
        if (valueNode == null || valueNode.isNull()) {
            throw new IllegalArgumentException("Missing or null field: " + fieldName);
        }
        return valueNode.asLong();
    }

    private String extractFileNameFromUrl(String url) {
        if (url == null || !url.contains("/")) {
            throw new IllegalArgumentException("Invalid URL format: " + url);
        }
        return url.substring(url.lastIndexOf('/') + 1);
    }

    private Map<String, Object> prepareMetadata(String fileName, String url, String storageAccount, String contentType, long fileSize, String blobType, String eventTime) {
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
        metadata.put("ExpiryTimestamp", EXPIRY_TIMESTAMP);
        return metadata;
    }

    private void logDataToTableStorage(String metadataJson, ExecutionContext context) {
        TableClient tableClient = new TableClientBuilder()
                .connectionString(STORAGE_CONNECTION_STRING)
                .tableName(TABLE_NAME)
                .buildClient();

        // Create the table if it doesn't exist
        try {
            tableClient.createTable();
            context.getLogger().info("Table created: " + TABLE_NAME);
        } catch (TableServiceException e) {
            if (e.getResponse().getStatusCode() != 409) { // 409 Conflict -> table already exists
                throw new RuntimeException("Failed to create table: " + e.getMessage(), e);
            }
        }

        String partitionKey = "FileMetadata"; // You can choose a partitioning strategy
        String rowKey = UUID.randomUUID().toString();

        TableEntity entity = new TableEntity(partitionKey, rowKey)
                .addProperty("Metadata", metadataJson)
                .addProperty("ExpiryTimestamp", EXPIRY_TIMESTAMP);

        tableClient.createEntity(entity);

        context.getLogger().info("File information logged into Azure Table Storage.");
    }

    private void sendMessageToServiceBus(String metadataJson, ExecutionContext context) {
        ServiceBusClientBuilder serviceBusClientBuilder = new ServiceBusClientBuilder()
                .connectionString(SERVICE_BUS_CONNECTION_STRING);

        ServiceBusSenderClient senderClient = serviceBusClientBuilder
                .sender()
                .queueName(SERVICE_BUS_QUEUE_NAME)
                .buildClient();

        ServiceBusMessage message = new ServiceBusMessage(metadataJson);

        try {
            senderClient.sendMessage(message);
            context.getLogger().info("Message sent to Service Bus queue.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to send message to Service Bus: " + e.getMessage(), e);
        } finally {
            senderClient.close();
        }
    }
}
