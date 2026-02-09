#!/usr/bin/env node

const { Client } = require('@opensearch-project/opensearch');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');
const { fromNodeProviderChain } = require('@aws-sdk/credential-providers');
const { S3Client, ListObjectsV2Command, GetObjectCommand, DeleteObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

// Configuration
const OPENSEARCH_NODE = process.env.OPENSEARCH_NODE;
const INDEX_NAME = 'pageseeker_response_opensearch';
const S3_BUCKET = process.env.S3_BUCKET || 'scamtify-pageseeker-data';
const S3_REGION = process.env.S3_REGION || 'ap-southeast-1';
const INITIAL_BATCH_SIZE = 25;
const MAX_PAYLOAD_MB = 2;
const MAX_RETRIES = 5;

// Initialize clients
const signer = new AwsSigv4Signer({
  getCredentials: fromNodeProviderChain(),
  region: S3_REGION,
  service: 'es'
});

const osClient = new Client({
  ...signer,
  node: OPENSEARCH_NODE,
  ssl: { rejectUnauthorized: false }
});

const s3Client = new S3Client({ region: S3_REGION });

// Ensure index exists
async function ensureIndex() {
  try {
    const exists = await osClient.indices.exists({ index: INDEX_NAME });
    if (!exists.body) {
      await osClient.indices.create({
        index: INDEX_NAME,
        body: {
          settings: {
            number_of_shards: 1,
            number_of_replicas: 0,
            refresh_interval: '30s'
          },
          mappings: {
            properties: {
              id: { type: 'keyword' },
              keyword: { type: 'keyword' },
              ad_id: { type: 'keyword' },
              ad_name: { type: 'text' },
              ad_caption: { type: 'text' },
              ad_risk_reason: { type: 'text' },
              created_at: { type: 'date' }
            }
          }
        }
      });
      console.log('✅ Index created successfully');
    }
  } catch (error) {
    console.error('❌ Error creating index:', error.message);
  }
}

// Dynamic micro-batch with payload control
async function dynamicMicroBatch(records, filename) {
  let finalBatchSize = 1;
  let maxTestSize = Math.min(INITIAL_BATCH_SIZE, records.length);
  
  // Find optimal batch size
  for (let testSize = 1; testSize <= maxTestSize; testSize++) {
    const testBatch = records.slice(0, testSize);
    const testBody = testBatch.flatMap(record => [
      { index: { _index: INDEX_NAME, _id: record.id.toString() } },
      record
    ]);
    
    const payloadSize = Buffer.byteLength(JSON.stringify(testBody), 'utf8');
    const payloadSizeMB = payloadSize / (1024 * 1024);
    
    if (payloadSizeMB <= MAX_PAYLOAD_MB) {
      finalBatchSize = testSize;
    } else {
      break;
    }
  }
  
  console.log(`📦 Using batch size: ${finalBatchSize} records`);
  
  // Process in batches
  let syncedCount = 0;
  for (let i = 0; i < records.length; i += finalBatchSize) {
    const batch = records.slice(i, i + finalBatchSize);
    
    const bulkBody = batch.flatMap(record => [
      { index: { _index: INDEX_NAME, _id: record.id.toString() } },
      record
    ]);
    
    try {
      const response = await osClient.bulk({ body: bulkBody });
      
      if (response.body.errors) {
        // Count individual successes even when some items fail
        const items = response.body.items || [];
        const successItems = items.filter(item => {
          const action = item.index || item.create || item.update;
          return action && action.status >= 200 && action.status < 300;
        });
        syncedCount += successItems.length;
        const failedCount = items.length - successItems.length;
        if (failedCount > 0) {
          console.error(`⚠️ Batch partial: ${successItems.length} ok, ${failedCount} failed`);
        }
      } else {
        syncedCount += batch.length;
      }
    } catch (error) {
      console.error(`❌ Batch error:`, error.message);
      // Retry once
      try {
        await new Promise(resolve => setTimeout(resolve, 2000));
        const retryResponse = await osClient.bulk({ body: bulkBody });
        if (!retryResponse.body.errors) {
          syncedCount += batch.length;
          console.log(`✅ Retry succeeded for ${batch.length} records`);
        } else {
          const items = retryResponse.body.items || [];
          const successItems = items.filter(item => {
            const action = item.index || item.create || item.update;
            return action && action.status >= 200 && action.status < 300;
          });
          syncedCount += successItems.length;
        }
      } catch (retryError) {
        console.error(`❌ Retry also failed:`, retryError.message);
      }
    }
  }
  
  return syncedCount;
}

// Sync single file
async function syncFile(filename) {
  try {
    console.log(`📁 Syncing file: ${filename}`);
    
    // Read from S3
    const getCommand = new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: filename
    });
    
    const response = await s3Client.send(getCommand);
    const fileContent = await response.Body.transformToString();
    
    // Parse JSONL
    const lines = fileContent.trim().split('\n');
    const records = lines.map(line => JSON.parse(line));
    
    console.log(`📊 Processing ${records.length} records`);
    
    // Sync to OpenSearch
    await ensureIndex();
    const syncedCount = await dynamicMicroBatch(records, filename);
    
    console.log(`✅ Synced ${syncedCount}/${records.length} records`);
    
    // เก็บ IDs ที่ sync ไป
    const syncedIds = records.map(record => record.id);
    
    // Delete file from S3 only if all records synced successfully
    if (syncedCount >= records.length) {
      const deleteCommand = new DeleteObjectCommand({
        Bucket: S3_BUCKET,
        Key: filename
      });
      await s3Client.send(deleteCommand);
      console.log(`🗑️ Deleted file: ${filename}`);
    } else {
      console.log(`⚠️ Keeping file ${filename} (${records.length - syncedCount} records not synced)`);
    }
    
    return {
      success: true,
      syncedRecords: syncedCount,
      syncedIds: syncedIds
    };
    
  } catch (error) {
    console.error(`❌ Failed to sync file ${filename}:`, error);
    return {
      success: false,
      error: error.message,
      syncedRecords: 0,
      syncedIds: []
    };
  }
}

// Sync all files
async function syncAll(workflowId = null) {
  try {
    console.log('🚀 Starting sync from S3 to OpenSearch...');
    
    // Get all files (paginated - S3 returns max 1000 per request)
    let files = [];
    let continuationToken = undefined;
    
    do {
      const listCommand = new ListObjectsV2Command({
        Bucket: S3_BUCKET,
        Prefix: 'unsynced_',
        ContinuationToken: continuationToken
      });
      
      const response = await s3Client.send(listCommand);
      if (response.Contents) {
        files = files.concat(response.Contents);
      }
      continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);
    
    if (files.length === 0) {
      console.log('✅ No files to sync');
      return { success: true, syncedFiles: 0, totalRecords: 0 };
    }
    
    // Sort files
    files.sort((a, b) => a.Key.localeCompare(b.Key));
    
    console.log(`📊 Found ${files.length} files to sync`);
    
    let totalSynced = 0;
    let successCount = 0;
    let allSyncedIds = new Set(); // เก็บ IDs ทั้งหมดที่ sync ไป
    
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const result = await syncFile(file.Key);
      
      if (result.success) {
        totalSynced += result.syncedRecords;
        successCount++;
        // เก็บ IDs ที่ sync ไป
        if (result.syncedIds) {
          result.syncedIds.forEach(id => allSyncedIds.add(id));
        }
      }
      
      const progress = ((i + 1) / files.length * 100).toFixed(1);
      console.log(`📊 Progress: ${progress}% (${successCount}/${files.length} files)`);
    }
    
    // เขียน sync IDs log ลง folder log
    if (allSyncedIds.size > 0) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const wfPrefix = workflowId ? `${workflowId}_` : '';
      const syncLogPath = `log/sync_log_ids_${wfPrefix}${timestamp}.json`;
      const syncLogContent = {
        timestamp: new Date().toISOString(),
        workflowId: workflowId || null,
        totalIds: allSyncedIds.size,
        syncedIds: Array.from(allSyncedIds)
      };
      
      const putCommand = new PutObjectCommand({
        Bucket: S3_BUCKET,
        Key: syncLogPath,
        Body: JSON.stringify(syncLogContent, null, 2),
        ContentType: 'application/json'
      });
      
      await s3Client.send(putCommand);
      console.log(`📝 Sync IDs log saved to log folder: ${syncLogPath}`);
      console.log(`📊 Total synced IDs: ${allSyncedIds.size}`);
    }
    
    console.log('🎉 Sync completed!');
    console.log(`📊 Total files: ${files.length}`);
    console.log(`✅ Successful: ${successCount}`);
    console.log(`📊 Total records synced: ${totalSynced}`);
    
    return {
      success: true,
      syncedFiles: successCount,
      totalFiles: files.length,
      totalRecords: totalSynced,
      syncedIdsCount: allSyncedIds.size
    };
    
  } catch (error) {
    console.error('❌ Sync failed:', error);
    return {
      success: false,
      error: error.message,
      syncedFiles: 0,
      totalFiles: 0,
      totalRecords: 0
    };
  }
}

if (require.main === module) {
  syncAll()
    .then(result => {
      console.log('✅ Sync completed successfully');
    })
    .catch(error => {
      console.error('❌ Sync failed:', error);
      process.exit(1);
    });
}

module.exports = { syncAll, syncFile };
