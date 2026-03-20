#!/usr/bin/env node

const { Client } = require('@opensearch-project/opensearch');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');
const { fromNodeProviderChain } = require('@aws-sdk/credential-providers');
const { S3Client, ListObjectsV2Command, GetObjectCommand, DeleteObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

// Configuration
const OPENSEARCH_NODE = process.env.OPENSEARCH_NODE;
const INDEX_NAME = 'pageseeker_response_opensearch_v2';
const S3_BUCKET = process.env.S3_BUCKET || 'scamtify-pageseeker-data';
const S3_REGION = process.env.S3_REGION || 'ap-southeast-1';
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const INITIAL_BATCH_SIZE = 25;
const MAX_PAYLOAD_MB = 2;
const MAX_RETRIES = 5;
const MAX_TEXT_CHARS = 6000; // stay safely under 8192 token limit

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

// Embedding: generate for a single text via OpenAI
async function getEmbedding(text, retryCount = 0) {
  try {
    const resp = await fetch('https://api.openai.com/v1/embeddings', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${OPENAI_API_KEY}`
      },
      body: JSON.stringify({ model: 'text-embedding-3-small', input: text })
    });
    const data = await resp.json();
    if (!data.data || !data.data[0]) {
      throw new Error('OpenAI API error: ' + JSON.stringify(data));
    }
    return data.data[0].embedding;
  } catch (error) {
    if (retryCount < MAX_RETRIES) {
      console.log(`   ⚠️ Embedding retry ${retryCount + 1}/${MAX_RETRIES}: ${error.message}`);
      await new Promise(r => setTimeout(r, 2000 * (retryCount + 1)));
      return getEmbedding(text, retryCount + 1);
    }
    throw error;
  }
}

// Build embedding text — truncate ad_caption if total exceeds token limit
function buildEmbeddingText(record) {
  const adName = record.ad_name || '';
  const adCaption = record.ad_caption || '';
  const otherLen = adName.length + 1; // +1 for space
  const captionBudget = Math.max(0, MAX_TEXT_CHARS - otherLen);
  const trimmedCaption = adCaption.length > captionBudget
    ? adCaption.substring(0, captionBudget)
    : adCaption;
  const parts = [adName, trimmedCaption].filter(Boolean);
  return parts.join(' ') || 'empty';
}

// Ensure index exists
async function ensureIndex() {
  try {
    const exists = await osClient.indices.exists({ index: INDEX_NAME });
    if (!exists.body) {
      await osClient.indices.create({
        index: INDEX_NAME,
        body: {
          settings: {
            'index.knn': true,
            number_of_shards: 1,
            number_of_replicas: 0,
            refresh_interval: '30s',
            analysis: {
              analyzer: {
                thai_analyzer: {
                  type: 'custom',
                  tokenizer: 'icu_tokenizer',
                  filter: ['lowercase']
                }
              }
            }
          },
          mappings: {
            properties: {
              id: { type: 'keyword' },
              keyword: { type: 'keyword' },
              ad_id: { type: 'keyword' },
              ad_name: { type: 'text', analyzer: 'thai_analyzer' },
              ad_caption: { type: 'text', analyzer: 'thai_analyzer' },
              ad_risk_reason: { type: 'text', analyzer: 'thai_analyzer' },
              collected_at: { type: 'date' },
              created_at: { type: 'date' },
              embedding: {
                type: 'knn_vector',
                dimension: 1536,
                method: {
                  name: 'hnsw',
                  space_type: 'cosinesimil',
                  engine: 'faiss'
                }
              }
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
    
    // Generate embeddings for each record
    const bulkBody = [];
    for (const record of batch) {
      let embedding = null;
      if (OPENAI_API_KEY) {
        try {
          const text = buildEmbeddingText(record);
          embedding = await getEmbedding(text);
        } catch (err) {
          console.error(`⚠️ Embedding failed for record ${record.id}: ${err.message}`);
        }
      }
      bulkBody.push({ index: { _index: INDEX_NAME, _id: record.id.toString() } });
      if (embedding) {
        bulkBody.push({ ...record, embedding });
      } else {
        bulkBody.push(record);
      }
    }
    
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
    });
}

module.exports = { syncAll, syncFile };
