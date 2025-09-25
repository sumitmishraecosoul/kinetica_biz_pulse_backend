import dotenv from 'dotenv';
import path from 'path';

// Register module aliases for runtime path resolution
import moduleAlias from 'module-alias';
moduleAlias.addAliases({
  '@': path.join(__dirname, '.'),
  '@/controllers': path.join(__dirname, 'controllers'),
  '@/models': path.join(__dirname, 'models'),
  '@/routes': path.join(__dirname, 'routes'),
  '@/services': path.join(__dirname, 'services'),
  '@/middleware': path.join(__dirname, 'middleware'),
  '@/utils': path.join(__dirname, 'utils'),
  '@/types': path.join(__dirname, 'types'),
  '@/config': path.join(__dirname, '../config')
});

// Load environment variables FIRST, before any other imports
// Try multiple common .env locations (later loads override earlier)
const envCandidates = [
  path.join(process.cwd(), '.env'),
  path.join(process.cwd(), 'server/.env'),
  path.join(__dirname, '../.env')
];
for (const p of envCandidates) {
  const loaded = dotenv.config({ path: p, override: true });
  if (loaded.parsed) {
    console.log(`Loaded env from: ${p}`);
  }
}

// Debug: Check if environment variables are loaded (mask secrets)
console.log('Environment check:');
console.log('AZURE_STORAGE_CONNECTION_STRING set:', Boolean(process.env.AZURE_STORAGE_CONNECTION_STRING));
console.log('NODE_ENV:', process.env.NODE_ENV);
console.log('PORT:', process.env.PORT);

import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';
import compression from 'compression';
import rateLimit from 'express-rate-limit';

// Import middleware
import { requestLogger, errorLogger } from '@/utils/logger';

// Import routes
import dashboardRoutes from '@/routes/dashboard';
import authRoutes from '@/routes/auth';
import userRoutes from '@/routes/users';

// Import services
import { cacheService } from '@/services/cacheService';
import { authMiddleware } from '@/middleware/auth';
import { getAzureService } from '@/services/azureService';

const app = express();
const PORT = process.env.PORT || 5001;
const NODE_ENV = process.env.NODE_ENV || 'development';

// Security middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com"],
      scriptSrc: ["'self'"],
      imgSrc: ["'self'", "data:", "https:"],
      fontSrc: ["'self'", "https://fonts.gstatic.com"],
      connectSrc: ["'self'", "https://kineticadbms.blob.core.windows.net"]
    }
  }
}));

// CORS configuration - More permissive for development
app.use(cors({
  origin: true, // Allow all origins in development
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'x-user-roles', 'x-allowed-business-areas', 'x-allowed-channels', 'x-allowed-brands', 'x-allowed-customers', 'Cache-Control', 'Pragma', 'Expires']
}));

// Rate limiting - DISABLED for development to prevent access issues
// const limiter = rateLimit({
//   windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS || '60000'), // 1 minute
//   max: parseInt(process.env.RATE_LIMIT_MAX_REQUESTS || '10000'), // limit each IP to 10000 requests per minute (increased)
//   message: {
//     success: false,
//     error: {
//       code: 'RATE_LIMIT_EXCEEDED',
//       message: 'Too many requests from this IP, please try again later.'
//     }
//   },
//   standardHeaders: true,
//   legacyHeaders: false,
// });

// app.use('/api/', limiter);

// Compression middleware
app.use(compression());

// Body parsing middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Logging middleware
app.use(morgan('combined'));
app.use(requestLogger);
// Auth (RLS) middleware
app.use(authMiddleware);

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    success: true,
    message: 'Kinetica Biz-Pulse API is running',
    timestamp: new Date().toISOString(),
    environment: NODE_ENV,
    version: process.env.npm_package_version || '1.0.0'
  });
});

// API routes
app.use('/api/v1/auth', authRoutes);
app.use('/api/v1/dashboard', dashboardRoutes);
app.use('/api/v1/users', userRoutes);

// Root endpoint
app.get('/api/v1', (req, res) => {
  res.json({
    success: true,
    message: 'Kinetica Biz-Pulse API',
    version: '1.0.0',
    endpoints: {
      dashboard: '/api/v1/dashboard',
      customers: '/api/v1/customers',
      brands: '/api/v1/brands',
      categories: '/api/v1/categories',
      analytics: '/api/v1/analytics'
    }
  });
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    success: false,
    error: {
      code: 'NOT_FOUND',
      message: `Route ${req.originalUrl} not found`
    }
  });
});

// Error handling middleware
app.use(errorLogger);

app.use((error: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  console.error('Unhandled error:', error);
  
  res.status(error.status || 500).json({
    success: false,
    error: {
      code: error.code || 'INTERNAL_SERVER_ERROR',
      message: error.message || 'An unexpected error occurred',
      ...(NODE_ENV === 'development' && { stack: error.stack })
    }
  });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully');
  await cacheService.disconnect();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully');
  await cacheService.disconnect();
  process.exit(0);
});

// Start server
const startServer = async () => {
  try {
    // Test Azure connection (non-fatal)
    console.log('Testing Azure connection...');
    const azureService = getAzureService();
    const connectionTest = await azureService.testConnection();
    if (!connectionTest) {
      console.warn('⚠️ Azure connection test failed. Server will still start; endpoints may return errors until Azure is configured.');
    } else {
      try {
        await azureService.getDataSummary(); // warm cache
        console.log('✅ Azure connection successful');
      } catch (e) {
        console.warn('⚠️ Unable to prefetch data summary. Continuing to start server.', e);
      }
    }

    // Test cache connection
    console.log('Testing cache connection...');
    await cacheService.set('test', 'test', 60);
    console.log('✅ Cache connection successful');

    app.listen(PORT, () => {
      console.log(`🚀 Kinetica Biz-Pulse API server running on port ${PORT}`);
      console.log(`📊 Environment: ${NODE_ENV}`);
      console.log(`🔗 Health check: http://localhost:${PORT}/health`);
      console.log(`📚 API docs: http://localhost:${PORT}/api/v1`);
    });
  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
};

startServer();

export default app;
