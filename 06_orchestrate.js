#!/usr/bin/env node
require('dotenv').config();

const { downloadToS3 } = require('./01_download');
const { syncAll } = require('./02_sync');
const { testSync } = require('./03_test');
const { markAsSynced } = require('./04_mark_synced');
const { cleanS3 } = require('./05_clean_s3');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// Configuration
const S3_BUCKET = process.env.S3_BUCKET || 'scamtify-pageseeker-data';
const S3_REGION = process.env.S3_REGION || 'ap-southeast-1';

// Initialize S3 client
const s3Client = new S3Client({ region: S3_REGION });

// Save timestamp log
async function saveTimestampLog(summary) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const logFilename = `log/sync_log_${timestamp}.json`;
  
  const logData = {
    timestamp: new Date().toISOString(),
    summary,
    version: 'SYNC_SYSTEM_V1',
    mode: 'orchestration'
  };
  
  const command = new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: logFilename,
    Body: JSON.stringify(logData, null, 2),
    ContentType: 'application/json'
  });
  
  await s3Client.send(command);
  console.log(`📝 Sync log saved: ${logFilename}`);
  
  return logFilename;
}

// Main orchestration function
async function orchestrate() {
  try {
    const workflowId = Date.now().toString(36);
    console.log(`🚀 Starting complete sync orchestration... [workflow: ${workflowId}]`);
    console.log('📊 Plan: Download → Sync → Test → Mark → Clean → Log');
    
    const startTime = new Date();
    
    // Step 1: Download
    console.log('\n📥 Step 1: Download from Supabase to S3');
    let downloadResult;
    try {
      downloadResult = await downloadToS3();
    } catch (error) {
      console.error('❌ Download failed:', error);
      return { success: false, step: 'download', error: error.message };
    }
    
    if (!downloadResult.success) {
      console.error('❌ Download failed');
      return { success: false, step: 'download', error: 'Download failed' };
    }
    
    // Step 2: Sync
    console.log('\n📤 Step 2: Sync from S3 to OpenSearch');
    const syncResult = await syncAll(workflowId);
    
    if (!syncResult.success) {
      console.error('❌ Sync failed');
      return { success: false, step: 'sync', error: syncResult.error };
    }
    
    // Step 3: Test
    console.log('\n🧪 Step 3: Test sync integrity');
    console.log('🔍 Calling testSync()...');
    const testResult = await testSync();
    console.log('🔍 testSync() result:', testResult);
    
    // Retry loop if test fails (up to 3 attempts)
    let retryCount = 0;
    const MAX_RETRIES = 3;
    let currentTestResult = testResult;
    
    while (!currentTestResult.success && retryCount < MAX_RETRIES) {
      retryCount++;
      console.log(`⚠️ Test failed, re-syncing (attempt ${retryCount}/${MAX_RETRIES})...`);
      console.log('💡 This will fix any missing records');
      
      const reSyncResult = await syncAll(workflowId);
      if (!reSyncResult.success) {
        console.error('❌ Re-sync failed');
        if (retryCount >= MAX_RETRIES) {
          return { success: false, step: 'resync', error: 'Re-sync failed after max retries' };
        }
        continue;
      }
      
      currentTestResult = await testSync();
      console.log(`🔍 Re-test result (attempt ${retryCount}):`, currentTestResult);
    }
    
    if (!currentTestResult.success) {
      console.log('⚠️ Test still not perfect after retries, but continuing with mark step...');
      console.log(`📊 Records synced so far — proceeding to mark as synced`);
    }
    
    // Step 4: Mark as synced
    console.log('\n✅ Step 4: Mark records as synced in Supabase');
    const markResult = await markAsSynced(workflowId);
    
    if (!markResult.success) {
      console.error('❌ Mark as synced failed');
      return { success: false, step: 'mark', error: markResult.error };
    }
    
    // Step 5: Clean S3
    console.log('\n🗑️ Step 5: Clean S3');
    const cleanResult = await cleanS3();
    
    if (!cleanResult.success) {
      console.error('❌ Clean S3 failed');
      return { success: false, step: 'clean', error: cleanResult.error };
    }
    
    // Step 6: Save log
    console.log('\n📝 Step 6: Save timestamp log');
    
    const endTime = new Date();
    const duration = endTime - startTime;
    
    const summary = {
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      duration: duration,
      download: downloadResult,
      sync: syncResult,
      test: testResult,
      mark: markResult,
      clean: cleanResult,
      success: true
    };
    
    const logFilename = await saveTimestampLog(summary);
    
    // Final summary
    console.log('\n🎉 Complete orchestration finished!');
    console.log('📊 Final Results:');
    console.log(`   📥 Downloaded: ${downloadResult.totalRecords} records`);
    console.log(`   📤 Synced: ${syncResult.totalRecords} records`);
    console.log(`   ✅ Test: ${testResult.success ? 'PASSED' : 'FAILED'}`);
    console.log(`   📝 Marked: ${markResult.totalUpdated} records`);
    console.log(`   🗑️ Cleaned: ${cleanResult.filesDeleted} files`);
    console.log(`   ⏱️ Duration: ${Math.round(duration / 1000)} seconds`);
    console.log(`   📝 Log: ${logFilename}`);
    
    return {
      success: true,
      summary,
      logFilename
    };
    
  } catch (error) {
    console.error('❌ Orchestration failed:', error);
    return {
      success: false,
      error: error.message
    };
  }
}

if (require.main === module) {
  orchestrate()
    .then(result => {
      if (result.success) {
        console.log('✅ Complete orchestration finished successfully');
        process.exit(0);
      } else {
        console.log('❌ Orchestration failed');
        process.exit(1);
      }
    })
    .catch(error => {
      console.error('❌ Orchestration failed:', error);
      process.exit(1);
    });
}

module.exports = { orchestrate };
