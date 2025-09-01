import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import bcrypt from 'bcryptjs';
import { User } from '@/models/user';
import { getAzureUserService } from './azureUserService';

const USERS_CSV_FILE = path.join(process.cwd(), 'server', 'database', 'User_directory.csv');

interface CSVUser {
  id: string;
  email: string;
  passwordHash: string;
  roles: string;
  allowedBusinessAreas: string;
  allowedChannels: string;
  allowedBrands: string;
  allowedCustomers: string;
  createdAt: string;
  updatedAt: string;
}

function ensureCSVFile() {
  const dir = path.dirname(USERS_CSV_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  if (!fs.existsSync(USERS_CSV_FILE)) {
    const header = 'id,email,passwordHash,roles,allowedBusinessAreas,allowedChannels,allowedBrands,allowedCustomers,createdAt,updatedAt\n';
    fs.writeFileSync(USERS_CSV_FILE, header, 'utf-8');
  }
}

function parseCSV(csvContent: string): CSVUser[] {
  const lines = csvContent.trim().split('\n');
  if (lines.length <= 1) return []; // Only header or empty file
  
  const headers = lines[0].split(',');
  const users: CSVUser[] = [];
  
  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split(',');
    if (values.length === headers.length) {
      const user: any = {};
      headers.forEach((header, index) => {
        user[header.trim()] = values[index]?.trim() || '';
      });
      users.push(user as CSVUser);
    }
  }
  
  return users;
}

function convertCSVToUser(csvUser: CSVUser): User {
  return {
    id: csvUser.id,
    email: csvUser.email,
    passwordHash: csvUser.passwordHash,
    roles: csvUser.roles ? csvUser.roles.split('|').filter(Boolean) : ['user'],
    allowedBusinessAreas: csvUser.allowedBusinessAreas ? csvUser.allowedBusinessAreas.split('|').filter(Boolean) : undefined,
    allowedChannels: csvUser.allowedChannels ? csvUser.allowedChannels.split('|').filter(Boolean) : undefined,
    allowedBrands: csvUser.allowedBrands ? csvUser.allowedBrands.split('|').filter(Boolean) : undefined,
    allowedCustomers: csvUser.allowedCustomers ? csvUser.allowedCustomers.split('|').filter(Boolean) : undefined,
    createdAt: csvUser.createdAt,
    updatedAt: csvUser.updatedAt,
  };
}

function convertUserToCSV(user: User): CSVUser {
  return {
    id: user.id,
    email: user.email,
    passwordHash: user.passwordHash,
    roles: user.roles ? user.roles.join('|') : 'user',
    allowedBusinessAreas: user.allowedBusinessAreas ? user.allowedBusinessAreas.join('|') : '',
    allowedChannels: user.allowedChannels ? user.allowedChannels.join('|') : '',
    allowedBrands: user.allowedBrands ? user.allowedBrands.join('|') : '',
    allowedCustomers: user.allowedCustomers ? user.allowedCustomers.join('|') : '',
    createdAt: user.createdAt,
    updatedAt: user.updatedAt,
  };
}

function writeCSV(users: CSVUser[]) {
  const header = 'id,email,passwordHash,roles,allowedBusinessAreas,allowedChannels,allowedBrands,allowedCustomers,createdAt,updatedAt\n';
  const csvContent = header + users.map(user => 
    `${user.id},${user.email},${user.passwordHash},${user.roles},${user.allowedBusinessAreas},${user.allowedChannels},${user.allowedBrands},${user.allowedCustomers},${user.createdAt},${user.updatedAt}`
  ).join('\n');
  
  fs.writeFileSync(USERS_CSV_FILE, csvContent, 'utf-8');
}

export class CSVUserService {
  async findByEmail(email: string): Promise<User | undefined> {
    ensureCSVFile();
    const csvContent = fs.readFileSync(USERS_CSV_FILE, 'utf-8');
    const csvUsers = parseCSV(csvContent);
    const csvUser = csvUsers.find(u => u.email.toLowerCase() === email.toLowerCase());
    return csvUser ? convertCSVToUser(csvUser) : undefined;
  }

  async findById(id: string): Promise<User | undefined> {
    ensureCSVFile();
    const csvContent = fs.readFileSync(USERS_CSV_FILE, 'utf-8');
    const csvUsers = parseCSV(csvContent);
    const csvUser = csvUsers.find(u => u.id === id);
    return csvUser ? convertCSVToUser(csvUser) : undefined;
  }

  async createUser(params: { email: string; password: string; roles?: string[]; scopes?: Partial<User> }): Promise<User> {
    ensureCSVFile();
    const csvContent = fs.readFileSync(USERS_CSV_FILE, 'utf-8');
    const csvUsers = parseCSV(csvContent);
    
    if (csvUsers.some(u => u.email.toLowerCase() === params.email.toLowerCase())) {
      throw new Error('EMAIL_EXISTS');
    }
    
    const id = crypto.randomUUID();
    const passwordHash = await bcrypt.hash(params.password, 10);
    const now = new Date().toISOString();
    
    const user: User = {
      id,
      email: params.email,
      passwordHash,
      roles: params.roles && params.roles.length ? params.roles : ['user'],
      allowedBusinessAreas: (params.scopes?.allowedBusinessAreas as any) || undefined,
      allowedChannels: (params.scopes?.allowedChannels as any) || undefined,
      allowedBrands: (params.scopes?.allowedBrands as any) || undefined,
      allowedCustomers: (params.scopes?.allowedCustomers as any) || undefined,
      createdAt: now,
      updatedAt: now,
    };
    
    const csvUser = convertUserToCSV(user);
    csvUsers.push(csvUser);
    writeCSV(csvUsers);
    
    // Upload to Azure after creating user
    try {
      const azureUserService = getAzureUserService();
      await azureUserService.uploadUserFile();
      console.log('✅ User data uploaded to Azure successfully');
    } catch (error) {
      console.error('❌ Failed to upload user data to Azure:', error);
    }
    
    return user;
  }

  async updateUser(id: string, updates: Partial<User>): Promise<User | undefined> {
    ensureCSVFile();
    const csvContent = fs.readFileSync(USERS_CSV_FILE, 'utf-8');
    const csvUsers = parseCSV(csvContent);
    
    const userIndex = csvUsers.findIndex(u => u.id === id);
    if (userIndex === -1) return undefined;
    
    const currentUser = convertCSVToUser(csvUsers[userIndex]);
    const updatedUser: User = {
      ...currentUser,
      ...updates,
      updatedAt: new Date().toISOString(),
    };
    
    csvUsers[userIndex] = convertUserToCSV(updatedUser);
    writeCSV(csvUsers);
    
    return updatedUser;
  }

  async verifyPassword(user: User, password: string): Promise<boolean> {
    return bcrypt.compare(password, user.passwordHash);
  }

  async getAllUsers(): Promise<User[]> {
    ensureCSVFile();
    const csvContent = fs.readFileSync(USERS_CSV_FILE, 'utf-8');
    const csvUsers = parseCSV(csvContent);
    return csvUsers.map(convertCSVToUser);
  }
}

export const csvUserService = new CSVUserService();
