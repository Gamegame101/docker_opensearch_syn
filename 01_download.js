#!/usr/bin/env node

const { createClient } = require('@supabase/supabase-js');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// Configuration
const S3_BUCKET = process.env.S3_BUCKET || 'scamtify-pageseeker-data';
const S3_REGION = process.env.S3_REGION || 'ap-southeast-1';
const RECORDS_PER_FILE = 100;

// Initialize clients
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY,
  {
    auth: { persistSession: false },
    db: { 
      timeout: 600000, // 10 นาที
      searchPath: 'api'
    }
  }
);

const s3Client = new S3Client({ region: S3_REGION });

// Upload to S3
async function uploadToS3(content, filename) {
  const command = new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: filename,
    Body: content,
    ContentType: 'application/json'
  });
  
  await s3Client.send(command);
  console.log(`✅ Uploaded to S3: ${filename}`);
}

// Download from Supabase to S3
async function downloadToS3() {
  try {
    console.log('🚀 Starting download from Supabase to S3...');
    console.log(`📊 Records per file: ${RECORDS_PER_FILE}`);
    
    let totalRecords = 0;
    let fileCount = 0;
    let lastId = 0;
    const s3Files = [];
    
    while (true) {
      // Fetch batch from Supabase
      const { data: records, error } = await supabase
        .schema('api')
        .from('pageseeker_response_opensearch')
        .select('id, keyword, ad_id, ad_name, ad_caption, ad_risk_reason, collected_at')
        .eq('opensearch_sync', false)
        .order('id', { ascending: true })
        .gt('id', lastId)
        .limit(RECORDS_PER_FILE);
      
      if (error) {
        console.error('❌ Error fetching records:', error);
        throw error;
      }
      
      if (records.length === 0) {
        console.log('✅ No more records to fetch');
        break;
      }
      
      // Create JSONL content
      const jsonlContent = records
        .map(record => JSON.stringify(record))
        .join('\n') + '\n';
      
      // Upload to S3
      const filename = `unsynced_${String(fileCount + 1).padStart(4, '0')}.jsonl`;
      await uploadToS3(jsonlContent, filename);
      
      // Track progress
      totalRecords += records.length;
      fileCount++;
      lastId = records[records.length - 1].id;
      s3Files.push(filename);
      
      console.log(`📊 Progress: ${totalRecords} records, ${fileCount} files`);
      console.log(`📁 Last ID processed: ${lastId}`);
    }
    
    // Save summary to both root and log folder
    const summary = {
      totalRecords,
      downloadedAt: new Date().toISOString(),
      s3Bucket: S3_BUCKET,
      s3Files,
      fileCount,
      recordsPerFile: RECORDS_PER_FILE,
      version: 'SYNC_SYSTEM_V1'
    };
    
    // Upload to log folder only
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const logSummaryPath = `log/download_summary_${timestamp}.json`;
    await uploadToS3(JSON.stringify(summary, null, 2), logSummaryPath);
    
    console.log(`📝 Download summary saved to log folder: ${logSummaryPath}`);
    
    console.log('🎉 Download completed!');
    console.log(`📊 Total records: ${totalRecords}`);
    console.log(`📊 Files created: ${fileCount}`);
    console.log(`📁 S3 bucket: ${S3_BUCKET}`);
    
    return {
      success: true,
      totalRecords,
      fileCount,
      files: s3Files
    };
    
  } catch (error) {
    console.error('❌ Download failed:', error);
    throw error;
  }
}

if (require.main === module) {
  downloadToS3()
    .then(result => {
      console.log('✅ Download completed successfully');
    })
    .catch(error => {
      console.error('❌ Download failed:', error);
      process.exit(1);
    });
}

module.exports = { downloadToS3 };
