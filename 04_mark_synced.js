const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { createClient } = require('@supabase/supabase-js');

const S3_BUCKET = process.env.S3_BUCKET || 'scamtify-pageseeker-data';
const S3_REGION = process.env.S3_REGION || 'ap-southeast-1';

const s3Client = new S3Client({ region: S3_REGION });

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY,
  {
    auth: { persistSession: false },
    db: { 
      timeout: 120000,
      searchPath: 'api'
    }
  }
);

async function markAsSynced(workflowId = null) {
  try {
    console.log('🔍 Marking records as synced in Supabase...');
    
    // หา sync log file ตาม workflowId (ถ้ามี)
    const prefix = workflowId 
      ? `log/sync_log_ids_${workflowId}_` 
      : 'log/sync_log_ids_';
    console.log(`🔎 Looking for sync logs with prefix: ${prefix}`);
    
    const listCommand = new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: prefix
    });
    
    const response = await s3Client.send(listCommand);
    const logFiles = response.Contents || [];
    
    if (logFiles.length === 0) {
      console.log('❌ No sync log files found');
      console.log('💡 ต้องรัน 02_sync.js ใหม่เพื่อสร้าง sync log');
      return { success: false, error: 'No sync log files found' };
    }
    
    // อ่าน sync log ทุกไฟล์ รวม IDs ทั้งหมด (กรณี sync หลายรอบ)
    logFiles.sort((a, b) => new Date(a.LastModified) - new Date(b.LastModified));
    console.log(`📝 Found ${logFiles.length} sync log file(s)`);
    
    const allIdsSet = new Set();
    
    for (const logFile of logFiles) {
      console.log(`  � Reading: ${logFile.Key}`);
      
      const getCommand = new GetObjectCommand({
        Bucket: S3_BUCKET,
        Key: logFile.Key
      });
      
      const logResponse = await s3Client.send(getCommand);
      const logContent = await logResponse.Body.transformToString();
      const syncLog = JSON.parse(logContent);
      
      const ids = syncLog.syncedIds || [];
      ids.forEach(id => allIdsSet.add(id));
      console.log(`     → ${ids.length} IDs (timestamp: ${syncLog.timestamp})`);
    }
    
    const syncedIds = Array.from(allIdsSet);
    console.log(`� Total unique IDs from all logs: ${syncedIds.length}`);
    
    // Mark records in batches
    const batchSize = 1000;
    let totalUpdated = 0;
    
    console.log(`🔄 Updating ${syncedIds.length} records in Supabase...`);
    
    for (let i = 0; i < syncedIds.length; i += batchSize) {
      const batch = syncedIds.slice(i, i + batchSize);
      
      const { data, error } = await supabase
        .schema('api')
        .from('pageseeker_response_opensearch')
        .update({ opensearch_sync: true })
        .in('id', batch);
      
      if (error) {
        console.error(`❌ Error updating batch ${i + 1}-${Math.min(i + batchSize, syncedIds.length)}:`, error);
        continue;
      }
      
      totalUpdated += batch.length;
      
      const progress = ((i + batchSize) / syncedIds.length * 100).toFixed(1);
      console.log(`📊 Progress: ${Math.min(progress, 100)}% (${totalUpdated}/${syncedIds.length} records)`);
    }
    
    console.log('✅ Mark as synced completed!');
    console.log(`📊 Total records updated: ${totalUpdated}`);
    
    return {
      success: true,
      totalUpdated: totalUpdated,
      totalIds: syncedIds.length,
      logFiles: logFiles.map(f => f.Key)
    };
    
  } catch (error) {
    console.error('❌ Mark as synced failed:', error);
    return {
      success: false,
      error: error.message,
      totalUpdated: 0
    };
  }
}

if (require.main === module) {
  markAsSynced()
    .then(result => {
      if (result.success) {
        console.log('✅ Mark as synced completed successfully');
      } else {
        console.log('❌ Mark as synced failed');
        process.exit(1);
      }
    })
    .catch(error => {
      console.error('❌ Unexpected error:', error);
      process.exit(1);
    });
}

module.exports = { markAsSynced };
