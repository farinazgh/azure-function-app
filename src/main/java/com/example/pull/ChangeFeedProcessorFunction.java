package com.example.pull;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.io.*;
import java.time.*;
import java.util.*;

import com.azure.storage.blob.*;
import com.azure.storage.blob.changefeed.*;
import com.azure.storage.blob.changefeed.models.*;
import com.azure.storage.blob.models.*;
import com.azure.messaging.servicebus.*;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ChangeFeedProcessorFunction {

    // Configuration settings
    private static final String STORAGE_CONNECTION_STRING = System.getenv("STORAGE_CONNECTION_STRING");
    private static final String SERVICE_BUS_CONNECTION_STRING = System.getenv("SERVICE_BUS_CONNECTION_STRING");
    private static final String SERVICE_BUS_QUEUE_NAME = System.getenv("SERVICE_BUS_QUEUE_NAME");
    private static final String CHECKPOINT_CONTAINER_NAME = "changefeedcheckpoints";
    private static final String CHECKPOINT_BLOB_NAME = "checkpoint.txt";

    @FunctionName("ProcessChangeFeed")
    public void run(
            @TimerTrigger(name = "changeFeedTrigger", schedule = "0 */5 * * * *") String timerInfo,
            final ExecutionContext context) {
        context.getLogger().info("Change Feed processing function executed at: " + OffsetDateTime.now());

        try {
            // Initialize BlobServiceClient
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(STORAGE_CONNECTION_STRING)
                    .buildClient();

            // Initialize BlobChangefeedClient
            BlobChangefeedClient changefeedClient = new BlobChangefeedClientBuilder(blobServiceClient).buildClient();

            // Create or get the checkpoint container
            BlobContainerClient checkpointContainerClient = blobServiceClient.getBlobContainerClient(CHECKPOINT_CONTAINER_NAME);
            if (!checkpointContainerClient.exists()) {
                checkpointContainerClient.create();
                context.getLogger().info("Created checkpoint container: " + CHECKPOINT_CONTAINER_NAME);
            }

            // Get the checkpoint blob client
            BlobClient checkpointBlobClient = checkpointContainerClient.getBlobClient(CHECKPOINT_BLOB_NAME);

            // Read the last processed cursor (if exists)
            String lastCursor = null;
            if (checkpointBlobClient.exists()) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                checkpointBlobClient.download(outputStream);
                lastCursor = outputStream.toString("UTF-8");
                context.getLogger().info("Read last cursor from checkpoint.");
            } else {
                context.getLogger().info("No existing checkpoint found. Starting from the beginning.");
            }

            // Build BlobChangefeedPagedIterable
            BlobChangefeedPagedIterable changefeedPagedIterable;
            if (lastCursor != null) {
                // Continue from the last cursor
                changefeedPagedIterable = changefeedClient.getEvents(lastCursor);
            } else {
                // Start from the beginning
                changefeedPagedIterable = changefeedClient.getEvents();
            }

            // Prepare Service Bus client
            ServiceBusClientBuilder serviceBusClientBuilder = new ServiceBusClientBuilder()
                    .connectionString(SERVICE_BUS_CONNECTION_STRING);

            ServiceBusSenderClient senderClient = serviceBusClientBuilder
                    .sender()
                    .queueName(SERVICE_BUS_QUEUE_NAME)
                    .buildClient();

            // Initialize ObjectMapper for JSON processing
            ObjectMapper objectMapper = new ObjectMapper();

            // Process events
            String newCursor = lastCursor;
            for (BlobChangefeedEvent event : changefeedPagedIterable) {
                // Process only blob created events
                if (event.getEventType() == BlobChangefeedEventType.BLOB_CREATED) {
                    // Extract blob information
                    BlobChangefeedEventData eventData = event.getData();

                    String url = eventData.getBlobUrl();
                    BlobType blobType = eventData.getBlobType();
                    OffsetDateTime eventTime = event.getEventTime();

                    // Get blob client
                    BlobClient blobClient = new BlobClientBuilder()
                            .endpoint(url)
                            .buildClient();

                    // Get blob properties
                    BlobProperties properties = blobClient.getProperties();

                    String fileName = blobClient.getBlobName();
                    String contentType = properties.getContentType();
                    long fileSize = properties.getBlobSize();

                    // Prepare metadata
                    Map<String, Object> metadata = new HashMap<>();
                    metadata.put("FileName", fileName);
                    metadata.put("Url", url);
                    metadata.put("ContentType", contentType);
                    metadata.put("FileSize", fileSize);
                    metadata.put("BlobType", String.valueOf(blobType));
                    metadata.put("EventTime", eventTime.toString());
                    metadata.put("ProcessingTime", OffsetDateTime.now(ZoneOffset.UTC).toString());

                    String messageBody = objectMapper.writeValueAsString(metadata);

                    // Send message to Service Bus
                    ServiceBusMessage message = new ServiceBusMessage(messageBody);
                    senderClient.sendMessage(message);

                    context.getLogger().info("Processed blob: " + fileName);
                }

                // Update the cursor
//                newCursor = changefeedClient.getCursor(event);
                //TODO this part does not work
            }

            senderClient.close();

            // Save the new cursor for next execution
            if (newCursor != null && !newCursor.equals(lastCursor)) {
                InputStream cursorStream = new ByteArrayInputStream(newCursor.getBytes("UTF-8"));
                checkpointBlobClient.upload(cursorStream, newCursor.length(), true);
                context.getLogger().info("Checkpoint updated.");
            } else {
                context.getLogger().info("No new events processed. Checkpoint remains the same.");
            }

        } catch (Exception e) {
            context.getLogger().severe("Error processing the change feed: " + e.getMessage());
        }
    }
}
