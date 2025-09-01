import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import { logger } from '@/utils/logger';

type UserContext = {
  id?: string;
  roles: string[];
  allowedBusinessAreas?: string[];
  allowedChannels?: string[];
  allowedBrands?: string[];
  allowedCustomers?: string[];
};

function parseCsvHeader(header?: string | string[]): string[] | undefined {
  if (!header) return undefined;
  const raw = Array.isArray(header) ? header.join(',') : header;
  const list = raw
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  return list.length ? list : undefined;
}

function deriveScopesFromRoles(roles: string[]): Partial<UserContext> {
  const allowedBusinessAreas = new Set<string>();
  const allowedChannels = new Set<string>();
  const allowedBrands = new Set<string>();
  const allowedCustomers = new Set<string>();

  for (const role of roles) {
    const r = role.toLowerCase();
    if (r === 'admin') {
      // admin means no restriction; return empty to signal unrestricted
      return {};
    }
    if (r === 'channel:roi') {
      allowedChannels.add('Grocery ROI');
      allowedChannels.add('Wholesale ROI');
    }
    if (r === 'channel:uk' || r === 'channel:ni/uk') {
      allowedChannels.add('Grocery NI/UK');
      allowedChannels.add('Wholesale NI/UK');
    }
    if (r === 'channel:international') {
      allowedChannels.add('International');
    }
    if (r === 'channel:online') {
      allowedChannels.add('Online');
    }
    if (r === 'channel:others') {
      allowedChannels.add('Sports & Others');
    }
    if (r === 'business:food') allowedBusinessAreas.add('Food');
    if (r === 'business:household') allowedBusinessAreas.add('Household');
    if (r === 'business:brillo') allowedBusinessAreas.add('Brillo');
    if (r === 'business:kinetica') allowedBusinessAreas.add('Kinetica');

    if (r.startsWith('brand:')) {
      const name = role.substring('brand:'.length).trim();
      if (name) allowedBrands.add(name);
    }
    if (r.startsWith('customer:')) {
      const name = role.substring('customer:'.length).trim();
      if (name) allowedCustomers.add(name);
    }
  }

  return {
    allowedBusinessAreas: allowedBusinessAreas.size ? Array.from(allowedBusinessAreas) : undefined,
    allowedChannels: allowedChannels.size ? Array.from(allowedChannels) : undefined,
    allowedBrands: allowedBrands.size ? Array.from(allowedBrands) : undefined,
    allowedCustomers: allowedCustomers.size ? Array.from(allowedCustomers) : undefined,
  };
}

export function authMiddleware(req: Request, _res: Response, next: NextFunction) {
  const user: UserContext = { roles: [] };

  try {
    // Prefer JWT if provided
    const authHeader = req.headers['authorization'] || req.headers['Authorization' as any];
    const token = typeof authHeader === 'string' && authHeader.startsWith('Bearer ')
      ? authHeader.substring('Bearer '.length)
      : undefined;

    if (token && process.env.JWT_SECRET) {
      const payload: any = jwt.verify(token, process.env.JWT_SECRET);
      user.id = payload.sub || payload.userId;
      user.roles = Array.isArray(payload.roles) ? payload.roles : (payload.role ? [payload.role] : []);
      user.allowedBusinessAreas = payload.allowedBusinessAreas;
      user.allowedChannels = payload.allowedChannels;
      user.allowedBrands = payload.allowedBrands;
      user.allowedCustomers = payload.allowedCustomers;
    } else {
      // Dev headers fallback
      const headerRoles = parseCsvHeader(req.headers['x-user-roles'] || req.headers['x-user-role']);
      user.roles = headerRoles || ['admin'];
      user.allowedBusinessAreas = parseCsvHeader(req.headers['x-allowed-business-areas']);
      user.allowedChannels = parseCsvHeader(req.headers['x-allowed-channels']);
      user.allowedBrands = parseCsvHeader(req.headers['x-allowed-brands']);
      user.allowedCustomers = parseCsvHeader(req.headers['x-allowed-customers']);
    }

    // Derive scopes from roles if explicit scopes not supplied
    if (
      !user.allowedBusinessAreas &&
      !user.allowedChannels &&
      !user.allowedBrands &&
      !user.allowedCustomers &&
      user.roles && user.roles.length
    ) {
      const derived = deriveScopesFromRoles(user.roles);
      user.allowedBusinessAreas = derived.allowedBusinessAreas;
      user.allowedChannels = derived.allowedChannels;
      user.allowedBrands = derived.allowedBrands;
      user.allowedCustomers = derived.allowedCustomers;
    }

    // Attach to request (typed via express augmentation)
    (req as any).user = user;
  } catch (error) {
    logger.warn('Auth middleware error; proceeding as guest', error);
    (req as any).user = { roles: ['guest'] } as UserContext;
  }

  next();
}


