const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');

// Test the CSV user service
async function testCSVAuth() {
  console.log('Testing CSV-based authentication...');
  
  const USERS_CSV_FILE = path.join(__dirname, 'server', 'database', 'User_directory.csv');
  
  // Check if file exists
  if (fs.existsSync(USERS_CSV_FILE)) {
    console.log('✅ User_directory.csv exists');
    const content = fs.readFileSync(USERS_CSV_FILE, 'utf-8');
    console.log('File content:');
    console.log(content);
  } else {
    console.log('❌ User_directory.csv does not exist');
  }
  
  // Test password hashing
  const testPassword = 'test123';
  const hashedPassword = await bcrypt.hash(testPassword, 10);
  console.log('Test password hash:', hashedPassword);
  
  // Test password verification
  const isValid = await bcrypt.compare(testPassword, hashedPassword);
  console.log('Password verification test:', isValid ? '✅ PASS' : '❌ FAIL');
  
  console.log('CSV auth test completed');
}

testCSVAuth().catch(console.error);
