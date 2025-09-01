const { getAzureUserService } = require('./src/services/azureUserService.ts');
const fs = require('fs');
const path = require('path');

async function testAzureUpload() {
  console.log('Testing Azure User Directory upload...\n');
  
  const azureUserService = getAzureUserService();
  
  try {
    // 1. Check if local file exists
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
    
    // 2. List all files in Azure Biz-Pulse folder
    console.log('📁 Files in Azure Biz-Pulse folder:');
    const files = await azureUserService.listFiles();
    files.forEach(file => console.log(`  - ${file}`));
    console.log('\n---\n');
    
    // 3. Check if User_directory.csv exists in Azure
    console.log('🔍 Checking if User_directory.csv exists in Azure...');
    const exists = await azureUserService.fileExists();
    console.log(exists ? '✅ File exists in Azure' : '❌ File does not exist in Azure');
    
    if (exists) {
      const properties = await azureUserService.getFileProperties();
      console.log('File properties:', properties);
    }
    console.log('\n---\n');
    
    // 4. Upload file to Azure
    console.log('📤 Uploading User_directory.csv to Azure...');
    const uploadSuccess = await azureUserService.uploadUserFile();
    console.log(uploadSuccess ? '✅ Upload successful' : '❌ Upload failed');
    console.log('Azure URL:', azureUserService.getAzureUrl());
    console.log('\n---\n');
    
    // 5. Download and verify
    if (uploadSuccess) {
      console.log('📥 Downloading from Azure to verify...');
      const downloadedContent = await azureUserService.downloadUserFile();
      if (downloadedContent) {
        console.log('✅ Download successful');
        console.log('Downloaded content:');
        console.log(downloadedContent);
      } else {
        console.log('❌ Download failed');
      }
    }
    
  } catch (error) {
    console.error('❌ Error during Azure test:', error);
  }
  
  console.log('\nAzure upload test completed!');
}

testAzureUpload().catch(console.error);
