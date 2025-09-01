const { BlobServiceClient } = require('@azure/storage-blob');
const fs = require('fs');
const path = require('path');

async function testAzureSimple() {
  console.log('Testing Azure connectivity...\n');
  
  try {
    // Azure configuration
    const connectionString = 'DefaultEndpointsProtocol=https;AccountName=kineticadbms;AccountKey=JfMzO69p3Ip+Sz+YkXxp7sHxZw0O/JunSaS5qKnSSQnxk1lPhwiQwnGyyJif7sGB01l9amAdvU/t+ASthIK/ZQ==;EndpointSuffix=core.windows.net';
    const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    const containerName = 'thrive-worklytics';
    const blobFolder = 'Biz-Pulse';
    const userFileName = 'User_directory.csv';
    
    console.log('🔗 Azure Account:', blobServiceClient.accountName);
    console.log('📦 Container:', containerName);
    console.log('📁 Folder:', blobFolder);
    console.log('📄 File:', userFileName);
    console.log('\n---\n');
    
    // 1. Check local file
    const localFilePath = path.join(__dirname, 'server', 'database', 'User_directory.csv');
    if (fs.existsSync(localFilePath)) {
      console.log('✅ Local User_directory.csv exists');
      const content = fs.readFileSync(localFilePath, 'utf-8');
      console.log('Local file content:');
      console.log(content);
      console.log('\n---\n');
    } else {
      console.log('❌ Local User_directory.csv does not exist');
      return;
    }
    
    // 2. List files in Azure
    console.log('📁 Listing files in Azure Biz-Pulse folder...');
    const containerClient = blobServiceClient.getContainerClient(containerName);
    const files = [];
    for await (const blob of containerClient.listBlobsFlat({ prefix: blobFolder })) {
      files.push(blob.name);
    }
    
    console.log('Files found:');
    files.forEach(file => console.log(`  - ${file}`));
    console.log('\n---\n');
    
    // 3. Check if User_directory.csv exists
    const blobPath = `${blobFolder}/${userFileName}`;
    const blobClient = containerClient.getBlobClient(blobPath);
    const exists = await blobClient.exists();
    console.log(exists ? '✅ User_directory.csv exists in Azure' : '❌ User_directory.csv does not exist in Azure');
    
    if (exists) {
      const properties = await blobClient.getProperties();
      console.log('File properties:', {
        lastModified: properties.lastModified,
        contentLength: properties.contentLength,
        contentType: properties.contentType
      });
    }
    console.log('\n---\n');
    
    // 4. Upload file
    console.log('📤 Uploading User_directory.csv to Azure...');
    const blockBlobClient = containerClient.getBlockBlobClient(blobPath);
    const fileContent = fs.readFileSync(localFilePath, 'utf-8');
    
    await blockBlobClient.upload(fileContent, fileContent.length, {
      blobHTTPHeaders: {
        blobContentType: 'text/csv'
      }
    });
    
    console.log('✅ Upload successful!');
    console.log('Azure URL:', `https://kineticadbms.blob.core.windows.net/${containerName}/${blobPath}`);
    console.log('\n---\n');
    
    // 5. Download and verify
    console.log('📥 Downloading from Azure to verify...');
    const downloadResponse = await blobClient.download();
    const chunks = [];
    for await (const chunk of downloadResponse.readableStreamBody) {
      chunks.push(chunk);
    }
    const downloadedContent = Buffer.concat(chunks).toString('utf-8');
    
    console.log('✅ Download successful');
    console.log('Downloaded content:');
    console.log(downloadedContent);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
  
  console.log('\nAzure test completed!');
}

testAzureSimple().catch(console.error);
