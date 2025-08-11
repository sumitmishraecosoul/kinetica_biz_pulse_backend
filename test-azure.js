require('dotenv').config({ path: './.env' });
const { BlobServiceClient } = require('@azure/storage-blob');

async function testAzureConnection() {
  try {
    console.log('🔍 Testing Azure Connection...');
    console.log('Account Name:', process.env.AZURE_STORAGE_ACCOUNT_NAME || 'kineticadbms');
    console.log('Container Name:', process.env.AZURE_CONTAINER_NAME || 'thrive-worklytics');
    console.log('Connection String Length:', process.env.AZURE_STORAGE_CONNECTION_STRING ? process.env.AZURE_STORAGE_CONNECTION_STRING.length : 'NOT FOUND');
    
    if (!process.env.AZURE_STORAGE_CONNECTION_STRING) {
      throw new Error('AZURE_STORAGE_CONNECTION_STRING not found in .env file');
    }

    // Create blob service client
    const blobServiceClient = BlobServiceClient.fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING);
    console.log('✅ BlobServiceClient created successfully');
    console.log('Account Name from client:', blobServiceClient.accountName);

    // Test container access
    const containerName = process.env.AZURE_CONTAINER_NAME || 'thrive-worklytics';
    const containerClient = blobServiceClient.getContainerClient(containerName);
    console.log(`🔍 Testing access to container: ${containerName}`);

    // List blobs (this will test authentication)
    console.log('📋 Listing blobs in container...');
    const blobs = containerClient.listBlobsFlat();
    let blobCount = 0;
    const blobNames = [];
    
    for await (const blob of blobs) {
      blobCount++;
      blobNames.push(blob.name);
      if (blobCount <= 5) {
        console.log(`  - ${blob.name} (${blob.properties.contentLength} bytes)`);
      }
      if (blobCount >= 10) break; // Limit to first 10 blobs
    }

    console.log(`✅ Successfully connected to Azure! Found ${blobCount} blobs in container.`);
    
    // Check for our specific file
    const targetFile = 'Biz-Pulse/Front Office Flash - YTD.csv';
    const fileExists = blobNames.some(name => name.includes('Front Office Flash - YTD.csv'));
    console.log(`📄 Target file found: ${fileExists ? 'YES' : 'NO'}`);
    
    if (fileExists) {
      console.log('🎉 Everything looks good! Your Azure connection is working.');
    } else {
      console.log('⚠️  Target file not found. Available files:');
      blobNames.forEach(name => console.log(`  - ${name}`));
    }

  } catch (error) {
    console.error('❌ Azure connection test failed:');
    console.error('Error Code:', error.code);
    console.error('Error Message:', error.message);
    console.error('Status Code:', error.statusCode);
    
    if (error.details) {
      console.error('Error Details:', error.details);
    }
    
    console.log('\n🔧 Troubleshooting tips:');
    console.log('1. Check if your Azure Storage Account Key is correct');
    console.log('2. Verify the account name matches your Azure portal');
    console.log('3. Ensure the container name is correct');
    console.log('4. Check if your Azure subscription is active');
    console.log('5. Verify network access (firewall rules)');
  }
}

testAzureConnection();




