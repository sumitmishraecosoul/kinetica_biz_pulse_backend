export const config = {
  allowSignup: process.env.ALLOW_SIGNUP ? process.env.ALLOW_SIGNUP === 'true' : (process.env.NODE_ENV !== 'production'),
  jwtSecret: (process.env.JWT_SECRET || 'dev-secret'),
  jwtExpiresIn: (process.env.JWT_EXPIRES_IN || '12h'),
  refreshJwtSecret: (process.env.REFRESH_JWT_SECRET || 'dev-refresh-secret'),
  refreshJwtExpiresIn: (process.env.REFRESH_JWT_EXPIRES_IN || '7d'),
  topNDefaultLimit: Number(process.env.TOPN_LIMIT_DEFAULT || 20),
  riskLowMarginThreshold: Number(process.env.RISK_LOW_MARGIN_THRESHOLD || 15),
  riskDecliningTrendThreshold: Number(process.env.RISK_DECLINING_TREND_THRESHOLD || -5),
  riskLowVolumeThreshold: Number(process.env.RISK_LOW_VOLUME_THRESHOLD || 10000),
};


