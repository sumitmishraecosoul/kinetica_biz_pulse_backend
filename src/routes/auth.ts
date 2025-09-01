import { Router } from 'express';
import jwt, { Secret, SignOptions } from 'jsonwebtoken';
import { csvUserService } from '@/services/csvUserService';
import { config } from '@/utils/config';

const router = Router();

const JWT_SECRET: Secret = config.jwtSecret as Secret;
const JWT_EXPIRES_IN: string | number = config.jwtExpiresIn;
const REFRESH_JWT_SECRET: Secret = config.refreshJwtSecret as Secret;
const REFRESH_JWT_EXPIRES_IN: string | number = config.refreshJwtExpiresIn;

// In-memory refresh token store (in production, use Redis or database)
const refreshTokens = new Set<string>();

router.post('/signup', async (req, res): Promise<void> => {
  if (!config.allowSignup) {
    res.status(403).json({ success: false, error: { code: 'SIGNUP_DISABLED', message: 'Signup is disabled' } });
    return;
  }
  try {
    const { email, password, roles, scopes } = req.body || {};
    if (!email || !password) {
      res.status(400).json({ success: false, error: { code: 'INVALID_INPUT', message: 'email and password required' } });
      return;
    }
    const user = await csvUserService.createUser({ email, password, roles, scopes });
    const token = jwt.sign(
      { sub: user.id, email: user.email, roles: user.roles, allowedBusinessAreas: user.allowedBusinessAreas, allowedChannels: user.allowedChannels, allowedBrands: user.allowedBrands, allowedCustomers: user.allowedCustomers },
      JWT_SECRET,
      { expiresIn: JWT_EXPIRES_IN } as SignOptions
    );
    const refreshToken = jwt.sign(
      { sub: user.id, type: 'refresh' },
      REFRESH_JWT_SECRET,
      { expiresIn: REFRESH_JWT_EXPIRES_IN } as SignOptions
    );
    refreshTokens.add(refreshToken);
    res.json({ success: true, data: { token, refreshToken, user: { id: user.id, email: user.email, roles: user.roles } } });
    return;
  } catch (error: any) {
    if (error?.message === 'EMAIL_EXISTS') {
      res.status(409).json({ success: false, error: { code: 'EMAIL_EXISTS', message: 'Email already registered' } });
      return;
    }
    res.status(500).json({ success: false, error: { code: 'SIGNUP_ERROR', message: 'Failed to signup' } });
    return;
  }
});

router.post('/signin', async (req, res): Promise<void> => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) {
      res.status(400).json({ success: false, error: { code: 'INVALID_INPUT', message: 'email and password required' } });
      return;
    }
    const user = await csvUserService.findByEmail(email);
    if (!user) {
      res.status(401).json({ success: false, error: { code: 'INVALID_CREDENTIALS', message: 'Invalid email or password' } });
      return;
    }
    const ok = await csvUserService.verifyPassword(user, password);
    if (!ok) {
      res.status(401).json({ success: false, error: { code: 'INVALID_CREDENTIALS', message: 'Invalid email or password' } });
      return;
    }
    const token = jwt.sign(
      { sub: user.id, email: user.email, roles: user.roles, allowedBusinessAreas: user.allowedBusinessAreas, allowedChannels: user.allowedChannels, allowedBrands: user.allowedBrands, allowedCustomers: user.allowedCustomers },
      JWT_SECRET,
      { expiresIn: JWT_EXPIRES_IN } as SignOptions
    );
    const refreshToken = jwt.sign(
      { sub: user.id, type: 'refresh' },
      REFRESH_JWT_SECRET,
      { expiresIn: REFRESH_JWT_EXPIRES_IN } as SignOptions
    );
    refreshTokens.add(refreshToken);
    res.json({ success: true, data: { token, refreshToken, user: { id: user.id, email: user.email, roles: user.roles } } });
    return;
  } catch {
    res.status(500).json({ success: false, error: { code: 'SIGNIN_ERROR', message: 'Failed to signin' } });
    return;
  }
});

router.post('/refresh', async (req, res): Promise<void> => {
  try {
    const { refreshToken } = req.body || {};
    if (!refreshToken) {
      res.status(400).json({ success: false, error: { code: 'INVALID_INPUT', message: 'refreshToken required' } });
      return;
    }

    if (!refreshTokens.has(refreshToken)) {
      res.status(401).json({ success: false, error: { code: 'INVALID_REFRESH_TOKEN', message: 'Invalid refresh token' } });
      return;
    }

    const decoded = jwt.verify(refreshToken, REFRESH_JWT_SECRET) as any;
    if (decoded.type !== 'refresh') {
      res.status(401).json({ success: false, error: { code: 'INVALID_REFRESH_TOKEN', message: 'Invalid refresh token' } });
      return;
    }

    const user = await csvUserService.findById(decoded.sub);
    if (!user) {
      res.status(401).json({ success: false, error: { code: 'USER_NOT_FOUND', message: 'User not found' } });
      return;
    }

    // Remove old refresh token
    refreshTokens.delete(refreshToken);

    // Generate new tokens
    const newToken = jwt.sign(
      { sub: user.id, email: user.email, roles: user.roles, allowedBusinessAreas: user.allowedBusinessAreas, allowedChannels: user.allowedChannels, allowedBrands: user.allowedBrands, allowedCustomers: user.allowedCustomers },
      JWT_SECRET,
      { expiresIn: JWT_EXPIRES_IN } as SignOptions
    );
    const newRefreshToken = jwt.sign(
      { sub: user.id, type: 'refresh' },
      REFRESH_JWT_SECRET,
      { expiresIn: REFRESH_JWT_EXPIRES_IN } as SignOptions
    );
    refreshTokens.add(newRefreshToken);

    res.json({ success: true, data: { token: newToken, refreshToken: newRefreshToken } });
    return;
  } catch (error: any) {
    if (error.name === 'TokenExpiredError') {
      res.status(401).json({ success: false, error: { code: 'REFRESH_TOKEN_EXPIRED', message: 'Refresh token expired' } });
      return;
    }
    res.status(500).json({ success: false, error: { code: 'REFRESH_ERROR', message: 'Failed to refresh token' } });
    return;
  }
});

router.post('/logout', async (req, res): Promise<void> => {
  try {
    const { refreshToken } = req.body || {};
    if (refreshToken) {
      refreshTokens.delete(refreshToken);
    }
    res.json({ success: true, message: 'Logged out successfully' });
    return;
  } catch {
    res.status(500).json({ success: false, error: { code: 'LOGOUT_ERROR', message: 'Failed to logout' } });
    return;
  }
});

export default router;


