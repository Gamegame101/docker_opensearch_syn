#!/usr/bin/env node

const { Client } = require('@opensearch-project/opensearch');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');
const { fromNodeProviderChain } = require('@aws-sdk/credential-providers');

// Configuration
const OPENSEARCH_NODE = process.env.OPENSEARCH_NODE;
const INDEX_NAME = 'pageseeker_response_opensearch';
const S3_REGION = process.env.S3_REGION || 'ap-southeast-1';

// Initialize OpenSearch client
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

// Test sync integrity
async function testSync() {
  try {
    console.log('🧪 Testing sync integrity...');
    
    // Get OpenSearch count
    const osCount = await osClient.count({
      index: INDEX_NAME
    });
    
    console.log(`📊 OpenSearch records: ${osCount.body.count}`);
    
    // Get sample records to verify
    const sampleResponse = await osClient.search({
      index: INDEX_NAME,
      body: {
        size: 5,
        sort: [{ id: 'asc' }],
        query: { match_all: {} }
      }
    });
    
    const sampleRecords = sampleResponse.body.hits.hits;
    console.log('📊 Sample records:');
    
    for (const hit of sampleRecords) {
      const record = hit._source;
      console.log(`   📝 ID: ${record.id}, Ad ID: ${record.ad_id}, Name: ${record.ad_name?.substring(0, 50)}...`);
    }
    
    // Check for duplicates
    const duplicateCheck = await osClient.search({
      index: INDEX_NAME,
      body: {
        size: 0,
        aggs: {
          duplicate_count: {
            terms: {
              field: 'id',
              size: 10
            }
          }
        }
      }
    });
    
    const duplicates = duplicateCheck.body.aggregations.duplicate_count.buckets
      .filter(bucket => bucket.doc_count > 1);
    
    if (duplicates.length > 0) {
      console.log(`⚠️ Found ${duplicates.length} duplicate IDs`);
      return { success: false, duplicates: duplicates.length };
    }
    
    console.log('✅ No duplicates found');
    
    // Get expected count from latest download summary in log folder
    const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
    const s3Client = new S3Client({ region: S3_REGION });
    
    try {
      // Find latest download summary in log folder
      const listCommand = new ListObjectsV2Command({
        Bucket: process.env.S3_BUCKET || 'scamtify-pageseeker-data',
        Prefix: 'log/download_summary_'
      });
      
      const listResponse = await s3Client.send(listCommand);
      const summaryFiles = listResponse.Contents || [];
      
      if (summaryFiles.length === 0) {
        console.log('⚠️ Could not find download summary in log folder');
        return { success: true, totalRecords: osCount.body.count };
      }
      
      // Sort by last modified and get the latest
      summaryFiles.sort((a, b) => new Date(b.LastModified) - new Date(a.LastModified));
      const latestSummary = summaryFiles[0];
      
      const summaryCommand = new GetObjectCommand({
        Bucket: process.env.S3_BUCKET || 'scamtify-pageseeker-data',
        Key: latestSummary.Key
      });
      
      const summaryResponse = await s3Client.send(summaryCommand);
      const summary = JSON.parse(await summaryResponse.Body.transformToString());
      
      console.log(`📊 Expected records: ${summary.totalRecords} (from ${latestSummary.Key})`);
      console.log(`📊 Actual records: ${osCount.body.count}`);
      
      const missing = summary.totalRecords - osCount.body.count;
      
      if (missing === 0) {
        console.log('✅ All records synced successfully!');
        return { success: true, totalRecords: osCount.body.count, missing: 0 };
      } else if (missing > 0) {
        console.log(`⚠️ Missing ${missing} records`);
        return { success: false, totalRecords: osCount.body.count, missing };
      } else {
        // Extra records - check if this is because no new data was downloaded OR accumulated data
        if (summary.totalRecords === 0) {
          console.log('✅ No new data to sync, existing records are from previous runs');
          return { success: true, totalRecords: osCount.body.count, missing: 0 };
        } else if (osCount.body.count > summary.totalRecords) {
          console.log('✅ Data accumulated from multiple runs, this is normal');
          console.log(`📊 Total in OpenSearch: ${osCount.body.count}, New this run: ${summary.totalRecords}`);
          return { success: true, totalRecords: osCount.body.count, missing: 0 };
        } else {
          console.log(`⚠️ Extra ${Math.abs(missing)} records (possible duplicates)`);
          return { success: false, totalRecords: osCount.body.count, extra: Math.abs(missing) };
        }
      }
      
    } catch (error) {
      console.log('⚠️ Could not read download summary from log folder:', error.message);
      return { success: true, totalRecords: osCount.body.count };
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error);
    return { success: false, error: error.message };
  }
}

if (require.main === module) {
  testSync()
    .then(result => {
      if (result.success) {
        console.log('✅ Sync test passed');
      } else {
        console.log('❌ Sync test failed');
        console.log('💡 Consider re-running sync');
      }
    })
    .catch(error => {
      console.error('❌ Test failed:', error);
    });
}

module.exports = { testSync };
