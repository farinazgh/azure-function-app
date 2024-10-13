package com.example;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.Context;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableClientBuilder;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class CleanupFunction {
    // Configuration settings
    private static final String STORAGE_CONNECTION_STRING = System.getenv("STORAGE_CONNECTION_STRING");
    private static final String TABLE_NAME = "FileMetadata";

    @FunctionName("CleanupExpiredEntries")
    public void run(
            @TimerTrigger(name = "cleanupTimer", schedule = "0 0 0 * * *") String timerInfo,
            final ExecutionContext context) {
        context.getLogger().info("Cleanup function executed at: " + OffsetDateTime.now());

        try {
            TableClient tableClient = new TableClientBuilder()
                    .connectionString(STORAGE_CONNECTION_STRING)
                    .tableName(TABLE_NAME)
                    .buildClient();

            // Define the filter to find expired entries
            String expiryTime = OffsetDateTime.now(ZoneOffset.UTC).toString();
            String filter = "ExpiryTimestamp lt '" + expiryTime + "'";

            // Query for expired entries
            PagedIterable<TableEntity> expiredEntities = tableClient.listEntities(new ListEntitiesOptions().setFilter(filter), null, Context.NONE);

            int deletedCount = 0;

            for (TableEntity entity : expiredEntities) {
                tableClient.deleteEntity(entity.getPartitionKey(), entity.getRowKey());
                deletedCount++;
            }

            context.getLogger().info("Deleted " + deletedCount + " expired entries from Azure Table Storage.");

        } catch (Exception e) {
            context.getLogger().severe("Error during cleanup: " + e.getMessage());
        }
    }
}
