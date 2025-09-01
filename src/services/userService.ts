import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import bcrypt from 'bcryptjs';
import { User } from '@/models/user';

const USERS_FILE = path.join(process.cwd(), 'server', 'database', 'users.json');

function ensureFile() {
  const dir = path.dirname(USERS_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  if (!fs.existsSync(USERS_FILE)) fs.writeFileSync(USERS_FILE, '[]', 'utf-8');
}

export class UserService {
  async findByEmail(email: string): Promise<User | undefined> {
    ensureFile();
    const users: User[] = JSON.parse(fs.readFileSync(USERS_FILE, 'utf-8'));
    return users.find(u => u.email.toLowerCase() === email.toLowerCase());
  }

  async findById(id: string): Promise<User | undefined> {
    ensureFile();
    const users: User[] = JSON.parse(fs.readFileSync(USERS_FILE, 'utf-8'));
    return users.find(u => u.id === id);
  }

  async createUser(params: { email: string; password: string; roles?: string[]; scopes?: Partial<User> }): Promise<User> {
    ensureFile();
    const users: User[] = JSON.parse(fs.readFileSync(USERS_FILE, 'utf-8'));
    if (users.some(u => u.email.toLowerCase() === params.email.toLowerCase())) {
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
    users.push(user);
    fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2));
    return user;
  }

  async verifyPassword(user: User, password: string): Promise<boolean> {
    return bcrypt.compare(password, user.passwordHash);
  }
}

export const userService = new UserService();


