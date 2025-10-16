require('dotenv').config({ path: './.env' });

console.log('🔍 Azure Configuration Debug Report');
console.log('=====================================');

// Check if .env file is loaded
console.log('\n📁 Environment File Check:');
console.log('Environment variables loaded:', Object.keys(process.env).filter(key => key.startsWith('AZURE_')).length > 0 ? '✅ YES' : '❌ NO');

// Check connection string
console.log('\n🔑 Connection String Check:');
const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
if (connectionString) {
    console.log('✅ Connection string found');
    console.log('Length:', connectionString.length, 'characters');
    
    // Parse connection string to extract components
    const parts = connectionString.split(';');
    const accountName = parts.find(part => part.startsWith('AccountName='))?.split('=')[1];
    const accountKey = parts.find(part => part.startsWith('AccountKey='))?.split('=')[1];
    
    console.log('Account Name from connection string:', accountName);
    console.log('Account Key length:', accountKey ? accountKey.length : 'NOT FOUND');
    console.log('Account Key starts with:', accountKey ? accountKey.substring(0, 10) + '...' : 'NOT FOUND');
} else {
    console.log('❌ Connection string NOT FOUND');
}

// Check individual environment variables
console.log('\n📋 Individual Environment Variables:');
console.log('AZURE_STORAGE_ACCOUNT_NAME:', process.env.AZURE_STORAGE_ACCOUNT_NAME || 'NOT SET');
console.log('AZURE_CONTAINER_NAME:', process.env.AZURE_CONTAINER_NAME || 'NOT SET (will use default: thrive-worklytics)');
console.log('AZURE_BLOB_FOLDER:', process.env.AZURE_BLOB_FOLDER || 'NOT SET (will use default: Biz-Pulse)');
// console.log('AZURE_CSV_FILENAME:', process.env.AZURE_CSV_FILENAME || 'NOT SET (will use default: Front Office Flash - YTD.csv)');
console.log('AZURE_CSV_FILENAME:', process.env.AZURE_CSV_FILENAME || 'NOT SET (will use default: yearly_data.csv)');

// Check for common issues
console.log('\n🔧 Common Issues Check:');
console.log('Connection string has spaces at start:', connectionString ? connectionString.startsWith(' ') : 'N/A');
console.log('Connection string has spaces at end:', connectionString ? connectionString.endsWith(' ') : 'N/A');
console.log('Connection string has quotes:', connectionString ? (connectionString.includes('"') || connectionString.includes("'")) : 'N/A');

// Expected values
console.log('\n📝 Expected Values (from your requirements):');
console.log('Account Name: kineticadbms');
console.log('Container Name: thrive-worklytics');
console.log('Blob Folder: Biz-Pulse');
// console.log('CSV File: Front Office Flash - YTD.csv');
console.log('CSV File: yearly_data.csv');

console.log('\n✅ Debug complete! Check the values above against your Azure Portal.');































