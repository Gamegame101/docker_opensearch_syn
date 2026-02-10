#!/usr/bin/env node

const { S3Client, ListObjectsV2Command, DeleteObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

// Configuration
const S3_BUCKET = process.env.S3_BUCKET || 'scamtify-pageseeker-data';
const S3_REGION = process.env.S3_REGION || 'ap-southeast-1';

// Initialize S3 client
const s3Client = new S3Client({ region: S3_REGION });

// Clean S3 (keep only summary and logs)
async function cleanS3() {
  try {
    console.log('🗑️ Cleaning S3 bucket...');
    
    // List all files (paginated - S3 returns max 1000 per request)
    let files = [];
    let continuationToken = undefined;
    
    do {
      const listCommand = new ListObjectsV2Command({
        Bucket: S3_BUCKET,
        ContinuationToken: continuationToken
      });
      
      const response = await s3Client.send(listCommand);
      if (response.Contents) {
        files = files.concat(response.Contents);
      }
      continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);
    
    console.log(`📊 Found ${files.length} files in S3`);
    
    // Separate files to keep vs delete
    const filesToKeep = files.filter(file => 
      file.Key.includes('summary') || 
      file.Key.includes('log/')
    );
    
    const filesToDelete = files.filter(file => 
      !file.Key.includes('summary') && 
      !file.Key.includes('log/')
    );
    
    console.log(`📁 Files to keep: ${filesToKeep.length}`);
    console.log(`🗑️ Files to delete: ${filesToDelete.length}`);
    
    // Delete files
    let deletedCount = 0;
    for (const file of filesToDelete) {
      const deleteCommand = new DeleteObjectCommand({
        Bucket: S3_BUCKET,
        Key: file.Key
      });
      
      await s3Client.send(deleteCommand);
      deletedCount++;
      
      if (deletedCount % 100 === 0) {
        console.log(`🗑️ Deleted ${deletedCount}/${filesToDelete.length} files`);
      }
    }
    
    console.log(`🗑️ Successfully deleted ${deletedCount} files`);
    
    // Save cleanup log
    const cleanupLog = {
      cleanedAt: new Date().toISOString(),
      totalFiles: files.length,
      filesDeleted: deletedCount,
      filesKept: filesToKeep.length,
      keptFiles: filesToKeep.map(f => f.Key),
      version: 'SYNC_SYSTEM_V1'
    };
    
    const logCommand = new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: `log/cleanup_log_${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
      Body: JSON.stringify(cleanupLog, null, 2),
      ContentType: 'application/json'
    });
    
    await s3Client.send(logCommand);
    console.log('📝 Cleanup log saved');
    
    return {
      success: true,
      totalFiles: files.length,
      filesDeleted: deletedCount,
      filesKept: filesToKeep.length
    };
    
  } catch (error) {
    console.error('❌ Cleanup failed:', error);
    return {
      success: false,
      error: error.message
    };
  }
}

if (require.main === module) {
  cleanS3()
    .then(result => {
      if (result.success) {
        console.log('✅ S3 cleanup completed successfully');
      } else {
        console.log('❌ S3 cleanup failed');
      }
    })
    .catch(error => {
      console.error('❌ S3 cleanup failed:', error);
    });
}

module.exports = { cleanS3 };
